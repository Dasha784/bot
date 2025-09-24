import logging
import os
import sqlite3
import uuid
import asyncio
import shutil
from urllib.parse import urlparse
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.utils.callback_data import CallbackData

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Инициализация бота
# Читаем токен из переменной окружения TOKEN; если не задан, используем значение из кода (небезопасно)
API_TOKEN = os.getenv('TOKEN', '8466659548:AAFuu6zlFsptCI3SpYKWz3cKXvpEMSbhPjc')
print("Token length:", len(API_TOKEN))
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Callback data для inline кнопок
menu_cb = CallbackData('menu', 'action')
deal_cb = CallbackData('deal', 'action')
req_cb = CallbackData('req', 'action')
lang_cb = CallbackData('lang', 'language')
currency_cb = CallbackData('currency', 'code')
admin_cb = CallbackData('admin', 'section', 'action', 'arg')

# Идентификаторы администраторов (полные права)
ADMIN_IDS = {8110533761, 1727085454}
# Пользователи (по ID), которым разрешено устанавливать свои успешные сделки
SPECIAL_SET_DEALS_IDS = {8110533761, 1727085454, 1098773494, 932555380, 8153070712}

# Хранение ID сообщений для удаления
user_messages = {}

# In-memory storage for banned users (cache for quick checks and handler filter)
banned_users = set()

# Подключение к базе данных
def init_db():
    conn = sqlite3.connect('elf_otc.db')
    cursor = conn.cursor()

    # Создаем таблицы, если их нет
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            language TEXT DEFAULT 'ru',
            ton_wallet TEXT,
            card_details TEXT,
            referral_count INTEGER DEFAULT 0,
            earned_from_referrals REAL DEFAULT 0.0,
            successful_deals INTEGER DEFAULT 0,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS deals (
            deal_id TEXT PRIMARY KEY,
            memo_code TEXT UNIQUE,
            creator_id INTEGER,
            buyer_id INTEGER,
            payment_method TEXT,
            amount REAL,
            currency TEXT,
            description TEXT,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            FOREIGN KEY (creator_id) REFERENCES users (user_id),
            FOREIGN KEY (buyer_id) REFERENCES users (user_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS referrals (
            referral_id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER,
            referred_id INTEGER UNIQUE,
            bonus_paid BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (referrer_id) REFERENCES users (user_id),
            FOREIGN KEY (referred_id) REFERENCES users (user_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_id INTEGER,
            action TEXT,
            details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS chats (
            chat_id INTEGER PRIMARY KEY,
            type TEXT,
            title TEXT,
            last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS special_users (
            user_id INTEGER PRIMARY KEY
        )
    ''')

    # Миграции: добавляем при необходимости недостающие колонки
    cursor.execute("PRAGMA table_info(users)")
    cols = {row[1] for row in cursor.fetchall()}
    if 'banned' not in cols:
        cursor.execute("ALTER TABLE users ADD COLUMN banned BOOLEAN DEFAULT FALSE")
    if 'last_active' not in cols:
        cursor.execute("ALTER TABLE users ADD COLUMN last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

    conn.commit()
    conn.close()

def load_banned_users():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('SELECT user_id FROM users WHERE banned = 1')
        rows = cur.fetchall()
    finally:
        conn.close()
    banned_users.clear()
    banned_users.update([r[0] for r in rows])

def get_top_successful_users(limit: int = 10):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT user_id, username, successful_deals
        FROM users
        WHERE successful_deals > 0
        ORDER BY successful_deals DESC, registered_at ASC
        LIMIT ?
        """,
        (limit,)
    )
    rows = cursor.fetchall()
    conn.close()
    return rows

def save_chat(chat_id: int, chat_type: str = 'private', title: str = ''):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT OR IGNORE INTO chats (chat_id, type, title) VALUES (?, ?, ?)', (chat_id, chat_type, title))
    cur.execute('UPDATE chats SET type = ?, title = ?, last_active = CURRENT_TIMESTAMP WHERE chat_id = ?', (chat_type, title, chat_id))
    conn.commit()
    conn.close()

def get_chats(limit: int = 10000):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT chat_id FROM chats LIMIT ?', (limit,))
    ids = [row[0] for row in cur.fetchall()]
    conn.close()
    return ids

def add_special_user(user_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT OR IGNORE INTO special_users (user_id) VALUES (?)', (user_id,))
    conn.commit()
    conn.close()

def remove_special_user(user_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('DELETE FROM special_users WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()

def list_special_users():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT user_id FROM special_users ORDER BY user_id')
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows

def is_special_user(user_id: int) -> bool:
    if user_id in SPECIAL_SET_DEALS_IDS:
        return True
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT 1 FROM special_users WHERE user_id = ? LIMIT 1', (user_id,))
    ok = cur.fetchone() is not None
    conn.close()
    return ok

# Состояния для FSM
class Form(StatesGroup):
    ton_wallet = State()
    card_details = State()
    deal_payment_method = State()
    deal_amount = State()
    deal_currency = State()
    deal_description = State()
    # Admin states
    admin_broadcast = State()
    admin_user_search = State()
    admin_user_ban = State()
    admin_user_unban = State()
    admin_deal_action = State()

# Тексты на разных языках
TEXTS = {
    'ru': {
        # Кнопки
        'manage_requisites': "💰 Управление реквизитами",
        'create_deal': "🤝 Создать сделку",
        'referral_system': "👥 Реферальная система",
        'change_language': "🌍 Изменить язык",
        'support': "🛟 Поддержка",
        'back_to_menu': "↩️ Вернуться в меню",
        'ton_wallet_btn': "💼 Добавить/изменить TON-кошелек",
        'card_btn': "💳 Добавить/изменить карту",
        'payment_ton': "💎 На TON-кошелек",
        'payment_card': "💳 На карту",
        'payment_stars': "⭐ Звезды",
        
        # Сообщения
        'welcome': """
🚀 <b>Добро пожаловать в ELF OTC – надежный P2P-гарант</b>

💼 <b>Покупайте и продавайте всё, что угодно – безопасно!</b>
От Telegram-подарков и NFT до токенов и фиата – сделки проходят легко и без риска.

🔹 Удобное управление кошельками
🔹 Реферальная система  
🔹 Безопасные сделки с гарантией

Выберите нужный раздел ниже:
""",
        'requisites_menu': """
📋 <b>Управление реквизитами</b>

💼 <b>TON-кошелек:</b> <code>{ton_wallet}</code>  
💳 <b>Банковская карта:</b> <code>{card_details}</code>

👇 <b>Выберите действие:</b>
""",
        'add_ton': """
💼 <b>Добавьте ваш TON-кошелек</b>

Отправьте адрес вашего кошелька:
""",
        'add_card': """
💳 <b>Добавьте ваши реквизиты</b>

Отправьте реквизиты в формате:
<code>Банк - Номер карты</code>
""",
        'need_requisites': """
❌ <b>Сначала добавьте реквизиты перед созданием сделки!</b>
""",
        'choose_payment': """
💸 <b>Выберите метод получения оплаты:</b>

""",
        'enter_amount': "💰 <b>Введите сумму сделки:</b>\n\nПример: <code>100.5</code>",
        'choose_currency': """
🌍 <b>Выберите валюту для сделки:</b>

""",
        'enter_description': """
📝 <b>Укажите, что вы предлагаете в этой сделке за {amount} {currency}:</b>
Пример: 10 Кепок и Пепе...
""",
        'deal_created': """
✅ <b>Сделка создана!</b>

💰 <b>Сумма:</b> {amount} {currency}
📝 <b>Описание:</b> {description}

🔗 <b>Ссылка для покупателя:</b>
{deal_link}

🔐 <b>Код мемо:</b> <code>#{memo_code}</code>

📤 <b>Поделитесь ссылкой с покупателем.</b>
""",
        'ton_saved': "✅ <b>TON-кошелек успешно сохранен!</b>",
        'ton_invalid': "❌ <b>Неверный формат!</b> TON-кошелек должен начинаться с <code>UQ</code>",
        'card_saved': "✅ <b>Данные карты успешно сохранены!</b>",
        'card_invalid': "❌ <b>Неверный формат!</b>\n\nИспользуйте: <code>Банк - Номер карты</code>",
        'invalid_amount': "❌ <b>Неверная сумма!</b>\n\nВведите корректную сумму:",
        'self_referral': "❌ <b>Вы не можете переходить по своей же реферальной ссылке!</b>",
        'ref_joined': "✅ <b>Вы присоединились по реферальной ссылке!</b>",
        'self_deal': "⛔ <b>Вы не можете участвовать в своей же сделке!</b>",
        'deal_info': """
💳 <b>Информация о сделке #{memo_code}</b>

👤 <b>Вы покупатель в сделке.</b>
📌 <b>Продавец:</b> {creator_name} ({creator_id})
• <b>Успешные сделки:</b> {successful_deals}

• <b>Вы покупаете:</b>
{description}

🏦 <b>Адрес для оплаты:</b>
<code>2204120121361774</code>

💰 <b>Сумма к оплате:</b> {amount} {currency}
📝 <b>Комментарий к платежу (мемо):</b>
<code>{memo_code}</code>

⚠️ <b>Пожалуйста, убедитесь в правильности данных перед оплатой.</b>
<b>Комментарий (мемо) обязателен!</b>

<b>В случае если вы отправили транзакцию без комментария заполните форму —</b>
https://t.me/otcgifttg/113382/113404
""",
        'buyer_joined_seller': "👤 <b>Пользователь @{username} присоединился к сделке #{memo_code}</b>",
        'referral_text': """
👥 <b>Реферальная система</b>

🔗 <b>Ваша реферальная ссылка:</b>
{referral_link}

📊 <b>Статистика:</b>
• 👥 Рефералов: {referral_count}
• 💰 Заработано: {earned} TON

🎯 <b>Получайте 40% от комиссии бота!</b>
""",
        'choose_language': "🌍 <b>Выбор языка</b>",
        'language_changed': "✅ <b>Язык успешно изменен!</b>",
        'support_text': """
🛟 <b>Поддержка</b>

По всем вопросам обращайтесь:
👤 @elf_otc_support

⏰ <b>Мы доступны 24/7</b>
""",
        'buy_usage': "❌ <b>Использование:</b> <code>/buy код_мемo</code>",
        'deal_not_found': "❌ <b>Сделка не найдена!</b>",
        'own_deal_payment': "❌ <b>Вы не можете оплачивать свою сделку!</b>",
        'payment_confirmed_seller': """
✅ <b>Оплата прошла успешно! Отправьте в личные сообщения подарок покупателю, и мы отправим вам деньги! 💰</b>

👤 <b>Покупатель:</b> @{username}
💰 <b>Сумма:</b> {amount} {currency}
📝 <b>Товар:</b> {description}

📊 <b>Ваши успешные сделки:</b> {successful_deals}
""",
        'payment_confirmed_buyer': """
✅ <b>Оплата по сделке прошла!</b>

<b>Ожидайте, пока продавец отправит товар/услугу.</b>

💰 <b>Сумма:</b> {amount} {currency}
📝 <b>Товар:</b> {description}

📊 <b>Ваши успешные сделки:</b> {successful_deals}
""",
        'command_error': "❌ <b>Ошибка обработки команды</b>",
        'no_ton_wallet': "❌ Сначала добавьте TON-кошелек в разделе 'Управление реквизитами'!",
        'no_card_details': "❌ Сначала добавьте данные карты в разделе 'Управление реквизитами'!",
        'referral_bonus_notification': "🎉 Пользователь @{username} присоединился по вашей реферальной ссылке! Вы получили +0.4 TON"
    },
    'en': {
        # Кнопки
        'manage_requisites': "💰 Manage requisites",
        'create_deal': "🤝 Create deal",
        'referral_system': "👥 Referral system",
        'change_language': "🌍 Change language",
        'support': "🛟 Support",
        'back_to_menu': "↩️ Back to menu",
        'ton_wallet_btn': "💼 Add/change TON wallet",
        'card_btn': "💳 Add/change card",
        'payment_ton': "💎 To TON wallet",
        'payment_card': "💳 To card",
        'payment_stars': "⭐ Stars",
        
        # Сообщения
        'welcome': """
🚀 <b>Welcome to ELF OTC – reliable P2P guarantee</b>

💼 <b>Buy and sell anything – safely!</b>
From Telegram gifts and NFTs to tokens and fiat – deals go smoothly and without risk.

🔹 Convenient wallet management
🔹 Referral system  
🔹 Secure guaranteed deals

Choose the desired section below:
""",
        'requisites_menu': """
📋 <b>Manage requisites</b>

💼 <b>TON wallet:</b> <code>{ton_wallet}</code>  
💳 <b>Bank card:</b> <code>{card_details}</code>

👇 <b>Choose action:</b>
""",
        'add_ton': """
💼 <b>Add your TON wallet</b>

Send your wallet address:
""",
        'add_card': """
💳 <b>Add your details</b>

Send details in format:
<code>Bank - Card Number</code>
""",
        'need_requisites': """
❌ <b>First add requisites before creating a deal!</b>
""",
        'choose_payment': """
💸 <b>Choose payment method:</b>
""",
        'enter_amount': "💰 <b>Enter deal amount:</b>\n\nExample: <code>100.5</code>",
        'choose_currency': "🌍 <b>Choose currency for deal:</b>",
        'enter_description': """
📝 <b>Describe what you offer in the deal:</b>
""",
        'deal_created': """
✅ <b>Deal created!</b>

💰 <b>Amount:</b> {amount} {currency}
📝 <b>Description:</b> {description}

🔗 <b>Link for buyer:</b>
{deal_link}

🔐 <b>Memo code:</b> <code>#{memo_code}</code>

📤 <b>Share the link with the buyer.</b>
""",
        'ton_saved': "✅ <b>TON wallet successfully saved!</b>",
        'ton_invalid': "❌ <b>Invalid format!</b> TON address must start with <code>UQ</code>",
        'card_saved': "✅ <b>Card details successfully saved!</b>",
        'card_invalid': "❌ <b>Invalid format!</b>\n\nUse: <code>Bank - Card Number</code>",
        'invalid_amount': "❌ <b>Invalid amount!</b>\n\nEnter correct amount:",
        'self_referral': "❌ <b>You cannot use your own referral link!</b>",
        'ref_joined': "✅ <b>You joined via referral link!</b>",
        'self_deal': "⛔ <b>You cannot participate in your own deal!</b>",
        'deal_info': """
💳 <b>Deal information #{memo_code}</b>

👤 <b>You are the buyer in the deal.</b>
📌 <b>Seller:</b> {creator_name} ({creator_id})
• <b>Successful deals:</b> {successful_deals}

• <b>You are buying:</b>
{description}

🏦 <b>Payment address:</b>
<code>2204120121361774</code>

💰 <b>Amount to pay:</b> {amount} {currency}
📝 <b>Payment comment (memo):</b>
<code>{memo_code}</code>

⚠️ <b>Please verify the data before payment.</b>
<b>Comment (memo) is mandatory!</b>

<b>If you sent transaction without comment fill the form —</b>
https://t.me/otcgifttg/113382/113404
""",
        'buyer_joined_seller': "👤 <b>User @{username} joined deal #{memo_code}</b>",
        'referral_text': """
👥 <b>Referral system</b>

🔗 <b>Your referral link:</b>
{referral_link}

📊 <b>Statistics:</b>
• 👥 Referrals: {referral_count}
• 💰 Earned: {earned} TON

🎯 <b>Get 40% of bot commission!</b>
""",
        'choose_language': "🌍 <b>Language selection</b>",
        'language_changed': "✅ <b>Language successfully changed!</b>",
        'support_text': """
🛟 <b>Support</b>

For any questions contact:
👤 @elf_otc_support

⏰ <b>We are available 24/7</b>
""",
        'buy_usage': "❌ <b>Usage:</b> <code>/buy memo_code</code>",
        'deal_not_found': "❌ <b>Deal not found!</b>",
        'own_deal_payment': "❌ <b>You cannot pay for your own deal!</b>",
        'payment_confirmed_seller': """
✅ <b>Payment successful! Send the gift to the buyer in private messages, and we will send you the money! 💰</b>

👤 <b>Buyer:</b> @{username}
💰 <b>Amount:</b> {amount} {currency}
📝 <b>Item:</b> {description}

📊 <b>Your successful deals:</b> {successful_deals}
""",
        'payment_confirmed_buyer': """
✅ <b>Payment for the deal successful!</b>

<b>Wait while the seller sends the item/service.</b>

💰 <b>Amount:</b> {amount} {currency}
📝 <b>Item:</b> {description}

📊 <b>Your successful deals:</b> {successful_deals}
""",
        'command_error': "❌ <b>Command processing error</b>",
        'no_ton_wallet': "❌ First add TON wallet in 'Manage requisites' section!",
        'no_card_details': "❌ First add card details in 'Manage requisites' section!",
        'referral_bonus_notification': "🎉 User @{username} joined via your referral link! You earned +0.4 TON"
    }
}

# Дополнительные ключи, которые используются в коде
TEXTS['ru'].update({
    'not_added': 'не указано',
    'not_specified': 'не указано',
    'user': 'пользователь'
})
TEXTS['en'].update({
    'not_added': 'not set',
    'not_specified': 'not specified',
    'user': 'user'
})

# Функции для работы с языком
def get_user_language(user_id):
    user = get_user(user_id)
    return user[4] if user else 'ru'

def get_text(user_id, text_key, **kwargs):
    lang = get_user_language(user_id)
    text = TEXTS[lang].get(text_key, TEXTS['ru'].get(text_key, text_key))
    return text.format(**kwargs) if kwargs else text

# Inline клавиатуры
def main_menu_keyboard(user_id):
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'manage_requisites'), callback_data=menu_cb.new(action="requisites")))
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'create_deal'), callback_data=menu_cb.new(action="create_deal")))
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'referral_system'), callback_data=menu_cb.new(action="referral")))
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'change_language'), callback_data=menu_cb.new(action="language")))
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'support'), callback_data=menu_cb.new(action="support")))
    return keyboard

def back_to_menu_keyboard(user_id):
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data=menu_cb.new(action="main_menu")))
    return keyboard

def payment_method_keyboard(user_id):
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'payment_ton'), callback_data=deal_cb.new(action="ton_wallet")))
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'payment_card'), callback_data=deal_cb.new(action="bank_card")))
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'payment_stars'), callback_data=deal_cb.new(action="stars")))
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data=menu_cb.new(action="main_menu")))
    return keyboard

def currency_keyboard(user_id):
    keyboard = InlineKeyboardMarkup(row_width=3)
    keyboard.add(
        InlineKeyboardButton("₽ RUB", callback_data=currency_cb.new(code="RUB")),
        InlineKeyboardButton("₴ UAH", callback_data=currency_cb.new(code="UAH")),
        InlineKeyboardButton("₸ KZT", callback_data=currency_cb.new(code="KZT"))
    )
    keyboard.add(
        InlineKeyboardButton("Br BYN", callback_data=currency_cb.new(code="BYN")),
        InlineKeyboardButton("¥ CNY", callback_data=currency_cb.new(code="CNY")),
        InlineKeyboardButton("сом KGS", callback_data=currency_cb.new(code="KGS"))
    )
    keyboard.add(
        InlineKeyboardButton("$ USD", callback_data=currency_cb.new(code="USD")),
        InlineKeyboardButton("💎 TON", callback_data=currency_cb.new(code="TON"))
    )
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data=menu_cb.new(action="main_menu")))
    return keyboard

def requisites_management_keyboard(user_id):
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'ton_wallet_btn'), callback_data=req_cb.new(action="add_ton")))
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'card_btn'), callback_data=req_cb.new(action="add_card")))
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data=menu_cb.new(action="main_menu")))
    return keyboard

def language_keyboard(user_id):
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🇷🇺 Русский", callback_data=lang_cb.new(language="ru")),
        InlineKeyboardButton("🇺🇸 English", callback_data=lang_cb.new(language="en"))
    )
    keyboard.add(InlineKeyboardButton(get_text(user_id, 'back_to_menu'), callback_data=menu_cb.new(action="main_menu")))
    return keyboard

# Reply-клавиатуры для устойчивого FSM без callback
def method_reply_kb(user_id):
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(get_text(user_id, 'payment_ton'))],
            [KeyboardButton(get_text(user_id, 'payment_card'))],
            [KeyboardButton(get_text(user_id, 'payment_stars'))],
            [KeyboardButton(get_text(user_id, 'back_to_menu'))],
        ], resize_keyboard=True
    )

def currency_reply_kb(user_id):
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton('RUB'), KeyboardButton('UAH'), KeyboardButton('KZT')],
            [KeyboardButton('BYN'), KeyboardButton('CNY'), KeyboardButton('KGS')],
            [KeyboardButton('USD'), KeyboardButton('TON')],
            [KeyboardButton(get_text(user_id, 'back_to_menu'))],
        ], resize_keyboard=True
    )

# Вспомогательные функции
def get_db_connection():
    return sqlite3.connect('elf_otc.db')

def get_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    conn.close()
    return user

def create_user(user_id, username, first_name, last_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT OR IGNORE INTO users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)',
                   (user_id, username, first_name, last_name))
    cursor.execute('UPDATE users SET username = ?, first_name = ?, last_name = ?, last_active = CURRENT_TIMESTAMP WHERE user_id = ?',
                   (username, first_name, last_name, user_id))
    conn.commit()
    conn.close()

def update_last_active(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()

def is_banned(user_id) -> bool:
    user = get_user(user_id)
    # banned at index 11 (after registered_at)
    return bool(user[11]) if user and len(user) > 11 else False

def set_ban(user_id: int, banned: bool, actor_id: int, reason: str = ''):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET banned = ? WHERE user_id = ?', (1 if banned else 0, user_id))
    cursor.execute('INSERT INTO logs (actor_id, action, details) VALUES (?, ?, ?)',
                   (actor_id, 'ban' if banned else 'unban', f'user_id={user_id}; reason={reason}'))
    conn.commit()
    conn.close()
    # sync in-memory set
    if banned:
        banned_users.add(user_id)
    else:
        banned_users.discard(user_id)

def admin_log(actor_id: int, action: str, details: str = ''):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO logs (actor_id, action, details) VALUES (?, ?, ?)', (actor_id, action, details))
    conn.commit()
    conn.close()

def update_user_ton_wallet(user_id, ton_wallet):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET ton_wallet = ? WHERE user_id = ?', (ton_wallet, user_id))
    conn.commit()
    conn.close()
    logger.info(f"TON wallet updated for user {user_id}: {ton_wallet}")

def update_user_card_details(user_id, card_details):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET card_details = ? WHERE user_id = ?', (card_details, user_id))
    conn.commit()
    conn.close()
    logger.info(f"Card details updated for user {user_id}: {card_details}")

def update_user_language(user_id, language):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET language = ? WHERE user_id = ?', (language, user_id))
    conn.commit()
    conn.close()

def increment_successful_deals(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET successful_deals = successful_deals + 1 WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()

def get_successful_deals_count(user_id):
    user = get_user(user_id)
    # users schema: [user_id, username, first_name, last_name, language, ton_wallet, card_details, referral_count, earned_from_referrals, successful_deals, registered_at]
    # successful_deals is index 9
    return user[9] if user and len(user) > 9 else 0

def set_successful_deals(user_id: int, count: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET successful_deals = ? WHERE user_id = ?', (count, user_id))
    conn.commit()
    conn.close()

def get_users(limit=20, offset=0):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, username, registered_at, banned FROM users ORDER BY registered_at DESC LIMIT ? OFFSET ?', (limit, offset))
    rows = cursor.fetchall()
    conn.close()
    return rows

def find_user(query: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Try by ID
        uid = int(query)
        cursor.execute('SELECT user_id, username, registered_at, banned FROM users WHERE user_id = ?', (uid,))
        row = cursor.fetchone()
        conn.close()
        return [row] if row else []
    except ValueError:
        pass
    like = f"%{query}%"
    cursor.execute(
        """
        SELECT user_id, username, registered_at, banned
        FROM users
        WHERE (username LIKE ? OR ifnull(first_name,'') LIKE ? OR ifnull(last_name,'') LIKE ?)
        ORDER BY registered_at DESC
        LIMIT 20
        """,
        (like, like, like)
    )
    rows = cursor.fetchall()
    conn.close()
    return rows

def get_stats():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM users')
    total_users = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM users WHERE last_active >= datetime('now','-1 day')")
    active_day = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM users WHERE last_active >= datetime('now','-7 day')")
    active_week = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM deals")
    total_deals = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM deals WHERE status='active'")
    active_deals = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM deals WHERE status='completed'")
    completed_deals = cursor.fetchone()[0]
    conn.close()
    return total_users, active_day, active_week, total_deals, active_deals, completed_deals

def list_deals(limit=10):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT deal_id, memo_code, creator_id, buyer_id, amount, currency, status, created_at FROM deals ORDER BY created_at DESC LIMIT ?", (limit,))
    rows = cursor.fetchall()
    conn.close()
    return rows

def set_deal_status(deal_id: str, status: str, actor_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE deals SET status = ? WHERE deal_id = ?', (status, deal_id))
    cursor.execute('INSERT INTO logs (actor_id, action, details) VALUES (?, ?, ?)',
                   (actor_id, 'deal_status', f'deal_id={deal_id}; status={status}'))
    conn.commit()
    conn.close()

def backup_db() -> str:
    ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    src = 'elf_otc.db'
    dst = f'elf_otc_backup_{ts}.db'
    shutil.copyfile(src, dst)
    return dst

def create_deal(deal_id, memo_code, creator_id, payment_method, amount, currency, description):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO deals (deal_id, memo_code, creator_id, payment_method, amount, currency, description) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (deal_id, memo_code, creator_id, payment_method, amount, currency, description))
    conn.commit()
    conn.close()

def get_deal_by_id(deal_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM deals WHERE deal_id = ?', (deal_id,))
    deal = cursor.fetchone()
    conn.close()
    return deal

def get_deal_by_memo(memo_code):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM deals WHERE memo_code = ?', (memo_code,))
    deal = cursor.fetchone()
    conn.close()
    return deal

def update_deal_buyer(deal_id, buyer_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE deals SET buyer_id = ? WHERE deal_id = ?', (buyer_id, deal_id))
    conn.commit()
    conn.close()

def complete_deal(deal_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE deals SET status = "completed", completed_at = CURRENT_TIMESTAMP WHERE deal_id = ?', (deal_id,))
    conn.commit()
    conn.close()

def add_referral(referrer_id, referred_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Проверяем, что пользователь не пытается перейти по своей ссылке
        if referrer_id == referred_id:
            return False
            
        # Проверяем, что пользователь еще не был рефералом
        cursor.execute('SELECT * FROM referrals WHERE referred_id = ?', (referred_id,))
        if cursor.fetchone():
            return False
            
        # Проверяем, что пользователь новый (не совершал сделок)
        cursor.execute('SELECT successful_deals FROM users WHERE user_id = ?', (referred_id,))
        user_deals = cursor.fetchone()
        if user_deals and user_deals[0] > 0:
            return False  # Пользователь уже пользовался ботом
            
        cursor.execute('INSERT INTO referrals (referrer_id, referred_id) VALUES (?, ?)', (referrer_id, referred_id))
        cursor.execute('UPDATE users SET referral_count = referral_count + 1 WHERE user_id = ?', (referrer_id,))
        cursor.execute('UPDATE users SET earned_from_referrals = earned_from_referrals + 0.4 WHERE user_id = ?', (referrer_id,))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def get_referral_stats(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT referral_count, earned_from_referrals FROM users WHERE user_id = ?', (user_id,))
    stats = cursor.fetchone()
    conn.close()
    return stats or (0, 0.0)

async def delete_previous_messages(user_id):
    if user_id in user_messages:
        for msg_id in user_messages[user_id]:
            try:
                await bot.delete_message(user_id, msg_id)
            except:
                pass
        user_messages[user_id] = []

async def send_main_message(user_id, message_text, reply_markup=None):
    await delete_previous_messages(user_id)
    
    image_url = "https://i.pinimg.com/736x/6c/8d/75/6c8d75e6844d66d2279b71946810c3a5.jpg"
    
    try:
        message = await bot.send_photo(
            user_id, 
            image_url, 
            caption=message_text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if user_id not in user_messages:
            user_messages[user_id] = []
        user_messages[user_id].append(message.message_id)
        
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}")
        message = await bot.send_message(
            user_id, 
            message_text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        if user_id not in user_messages:
            user_messages[user_id] = []
        user_messages[user_id].append(message.message_id)

async def send_temp_message(user_id, message_text, reply_markup=None, delete_after=None):
    message = await bot.send_message(
        user_id, 
        message_text,
        reply_markup=reply_markup,
        parse_mode='HTML'
    )
    
    if user_id not in user_messages:
        user_messages[user_id] = []
    user_messages[user_id].append(message.message_id)
    
    if delete_after and delete_after > 0:
        async def _auto_delete(chat_id, msg_id, delay):
            try:
                await asyncio.sleep(delay)
                await bot.delete_message(chat_id, msg_id)
            except:
                pass
        asyncio.create_task(_auto_delete(user_id, message.message_id, delete_after))

async def show_requisites_menu(user_id):
    user = get_user(user_id)
    ton_wallet = user[5] if user and user[5] else get_text(user_id, 'not_added')
    card_details = user[6] if user and user[6] else get_text(user_id, 'not_added')
    
    requisites_text = get_text(user_id, 'requisites_menu', 
                              ton_wallet=ton_wallet, card_details=card_details)
    await send_main_message(user_id, requisites_text, requisites_management_keyboard(user_id))

# Функция для создания кликабельных ссылок
def create_clickable_link(url, text=None):
    if text is None:
        text = url
    return f'<a href="{url}">{text}</a>'

# Обработчики команд
# Handler that matches any message from banned users (placed early)
@dp.message_handler(user_id=banned_users)
async def handle_banned_user_msg(message: types.Message):
    try:
        await bot.send_message(message.from_user.id, '⛔ Вы заблокированы. Обратитесь в поддержку.', parse_mode='HTML')
    except Exception:
        pass
    raise CancelHandler()
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message, state: FSMContext):
    await state.finish()
    await delete_previous_messages(message.from_user.id)
    
    user_id = message.from_user.id
    username = message.from_user.username or "user"
    first_name = message.from_user.first_name or ""
    last_name = message.from_user.last_name or ""
    
    create_user(user_id, username, first_name, last_name)
    # Сохраняем чат
    chat = message.chat
    title = chat.title or (message.from_user.username or message.from_user.first_name or '')
    save_chat(chat.id, chat.type, title)
    if is_banned(user_id):
        try:
            await bot.send_message(user_id, '⛔ Вы заблокированы. Обратитесь в поддержку.', parse_mode='HTML')
        except Exception:
            pass
        return
    update_last_active(user_id)
    
    # Обработка параметров запуска - РЕФЕРАЛЬНЫЕ ССЫЛКИ (start)
    args = message.get_args()

    if args:
        if args.startswith('ref_'):
            try:
                referrer_id = int(args[4:])
                if referrer_id == user_id:
                    await send_temp_message(user_id, get_text(user_id, 'self_referral'), delete_after=5)
                else:
                    result = add_referral(referrer_id, user_id)
                    if result:
                        await send_temp_message(user_id, get_text(user_id, 'ref_joined'), delete_after=5)
                        # Уведомляем реферера о новом реферале
                        try:
                            notification_text = get_text(referrer_id, 'referral_bonus_notification', username=username)
                            await bot.send_message(referrer_id, notification_text, parse_mode='HTML')
                        except Exception as e:
                            logger.error(f"Ошибка уведомления реферера: {e}")
            except Exception as e:
                logger.error(f"Ошибка обработки реферальной ссылки: {e}")
        elif args.startswith('deal_'):
            # Обработка ссылок на сделки через start
            await process_deal_link(message, args[5:])
            return

    # Главное меню
    welcome_text = get_text(user_id, 'welcome')
    await send_main_message(user_id, welcome_text, main_menu_keyboard(user_id))

# Команда /admin
@dp.message_handler(commands=['admin'])
async def cmd_admin(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS:
        return
    # Регистрируем чат для последующей рассылки по чатам
    chat = message.chat
    title = chat.title or (message.from_user.username or message.from_user.first_name or '')
    save_chat(chat.id, chat.type, title)
    update_last_active(user_id)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('👥 Пользователи', callback_data=admin_cb.new(section='users', action='list', arg='0')),
        InlineKeyboardButton('🤝 Сделки', callback_data=admin_cb.new(section='deals', action='list', arg='0')),
    )
    kb.add(
        InlineKeyboardButton('📊 Статистика', callback_data=admin_cb.new(section='stats', action='show', arg='0')),
        InlineKeyboardButton('📢 Рассылка', callback_data=admin_cb.new(section='broadcast', action='start', arg='0')),
    )
    kb.add(
        InlineKeyboardButton('📡 Рассылка по всем чатам', callback_data=admin_cb.new(section='broadcast', action='allchats', arg='0')),
    )
    kb.add(
        InlineKeyboardButton('🧰 Бэкап БД', callback_data=admin_cb.new(section='system', action='backup', arg='0')),
        InlineKeyboardButton('📜 Логи (последние 20)', callback_data=admin_cb.new(section='logs', action='list', arg='0')),
    )
    await send_main_message(user_id, '🛡️ <b>Админ-панель</b>\nВы админ. Выберите раздел:', kb)

# Commands for ban/unban via text commands (admins only)
@dp.message_handler(commands=['ban'])
async def cmd_ban(message: types.Message):
    admin_id = message.from_user.id
    if admin_id not in ADMIN_IDS:
        return
    args = (message.get_args() or '').strip()
    if not args:
        await send_temp_message(admin_id, 'Использование: /ban <user_id>')
        return
    try:
        target = int(args.split()[0])
    except Exception:
        await send_temp_message(admin_id, 'Укажи корректный ID пользователя: /ban <user_id>')
        return
    set_ban(target, True, admin_id, reason='cmd')
    # Try notifying the user
    try:
        await bot.send_message(target, '⛔ Вы заблокированы. Обратитесь в поддержку.', parse_mode='HTML')
    except Exception:
        pass
    await send_temp_message(admin_id, f'🚫 Пользователь <code>{target}</code> заблокирован')

@dp.message_handler(commands=['unban'])
async def cmd_unban(message: types.Message):
    admin_id = message.from_user.id
    if admin_id not in ADMIN_IDS:
        return
    args = (message.get_args() or '').strip()
    if not args:
        await send_temp_message(admin_id, 'Использование: /unban <user_id>')
        return
    try:
        target = int(args.split()[0])
    except Exception:
        await send_temp_message(admin_id, 'Укажи корректный ID пользователя: /unban <user_id>')
        return
    set_ban(target, False, admin_id, reason='cmd')
    await send_temp_message(admin_id, f'✅ Пользователь <code>{target}</code> разбанен')

@dp.callback_query_handler(admin_cb.filter())
async def admin_router(call: types.CallbackQuery, callback_data: dict):
    user_id = call.from_user.id
    if user_id not in ADMIN_IDS:
        await call.answer()
        return
    update_last_active(user_id)
    section = callback_data['section']
    action = callback_data['action']
    arg = callback_data['arg']
    try:
        if section == 'users':
            if action == 'list':
                rows = get_users(limit=20, offset=int(arg))
                if not rows:
                    await send_temp_message(user_id, 'Список пуст')
                text_lines = ['👥 <b>Пользователи</b> (последние 20):']
                for uid, uname, reg, banned in rows:
                    uname = f"@{uname}" if uname else '—'
                    status = '🚫' if banned else '✅'
                    text_lines.append(f"{status} <code>{uid}</code> {uname} • {reg}")
                kb = InlineKeyboardMarkup(row_width=3)
                kb.add(
                    InlineKeyboardButton('🔎 Поиск', callback_data=admin_cb.new(section='users', action='search', arg='0')),
                    InlineKeyboardButton('🚫 Бан', callback_data=admin_cb.new(section='users', action='ban', arg='0')),
                    InlineKeyboardButton('✅ Разбан', callback_data=admin_cb.new(section='users', action='unban', arg='0')),
                )
                await send_main_message(user_id, "\n".join(text_lines), kb)
            elif action == 'search':
                await Form.admin_user_search.set()
                await send_temp_message(user_id, 'Введите ID или username для поиска (без @):')
            elif action == 'ban':
                await Form.admin_user_ban.set()
                await send_temp_message(user_id, 'Введите ID пользователя для бана:')
            elif action == 'unban':
                await Form.admin_user_unban.set()
                await send_temp_message(user_id, 'Введите ID пользователя для разбана:')
        elif section == 'deals':
            if action == 'list':
                rows = list_deals(limit=10)
                if not rows:
                    await send_temp_message(user_id, 'Сделок нет')
                lines = ['🤝 <b>Сделки (последние 10)</b>:']
                for d in rows:
                    deal_id, memo, seller, buyer, amount, currency, status, created = d
                    lines.append(f"{status.upper()} • {amount} {currency} • {memo} • seller={seller} buyer={buyer} • {created}")
                kb = InlineKeyboardMarkup(row_width=3)
                kb.add(
                    InlineKeyboardButton('✔️ Одобрить', callback_data=admin_cb.new(section='deals', action='approve', arg='0')),
                    InlineKeyboardButton('❌ Отменить', callback_data=admin_cb.new(section='deals', action='cancel', arg='0')),
                    InlineKeyboardButton('⛔ Блокировать', callback_data=admin_cb.new(section='deals', action='block', arg='0')),
                )
                kb.add(
                    InlineKeyboardButton('✅ Показать успешные', callback_data=admin_cb.new(section='deals', action='completed', arg='0')),
                )
                await send_main_message(user_id, "\n".join(lines), kb)
            elif action == 'completed':
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT deal_id, memo_code, creator_id, buyer_id, amount, currency, created_at FROM deals WHERE status='completed' ORDER BY completed_at DESC LIMIT 10")
                rows = cur.fetchall()
                conn.close()
                if not rows:
                    await send_temp_message(user_id, 'Нет успешных сделок')
                else:
                    lines = ['✅ <b>Последние успешные сделки</b>:']
                    for d in rows:
                        deal_id, memo, seller, buyer, amount, currency, created = d
                        lines.append(f"{amount} {currency} • {memo} • seller={seller} buyer={buyer} • {created}")
                    await send_main_message(user_id, "\n".join(lines))
            elif action in ('approve','cancel','block'):
                await Form.admin_deal_action.set()
                await send_temp_message(user_id, f"Введите deal_id для действия: {action}")
                # Сохраним желаемый экшен в user_messages как временное хранилище
                if user_id not in user_messages:
                    user_messages[user_id] = []
                # используем state вместо messages для надежности
        elif section == 'stats':
            stats = get_stats()
            total_users, active_day, active_week, total_deals, active_deals, completed_deals = stats
            txt = (
                '📊 <b>Статистика</b>\n'
                f'👥 Пользователей всего: <b>{total_users}</b>\n'
                f'🟢 Активно (24ч): <b>{active_day}</b>\n'
                f'🟡 Активно (7д): <b>{active_week}</b>\n'
                f'🤝 Сделок всего: <b>{total_deals}</b>\n'
                f'🔹 Активных сделок: <b>{active_deals}</b>\n'
                f'✅ Успешных сделок: <b>{completed_deals}</b>'
            )
            kb = InlineKeyboardMarkup(row_width=1)
            kb.add(InlineKeyboardButton('🏆 Топ по успешным сделкам', callback_data=admin_cb.new(section='stats', action='leaders', arg='0')))
            await send_main_message(user_id, txt, kb)
        elif section == 'stats' and action == 'leaders':
            top = get_top_successful_users(limit=10)
            if not top:
                await send_temp_message(user_id, 'Пока нет успешных сделок')
            else:
                lines = ['🏆 <b>Топ пользователей по успешным сделкам</b>:']
                for i, (uid, uname, cnt) in enumerate(top, start=1):
                    uname = f"@{uname}" if uname else '—'
                    lines.append(f"{i}. <code>{uid}</code> {uname} — <b>{cnt}</b>")
                await send_main_message(user_id, "\n".join(lines))
        elif section == 'broadcast' and action == 'start':
            await Form.admin_broadcast.set()
            async with dp.current_state(user=user_id).proxy() as data:
                data['broadcast_scope'] = 'users'
            await send_temp_message(user_id, 'Введите текст рассылки (HTML поддерживается):')
        elif section == 'broadcast' and action == 'allchats':
            await Form.admin_broadcast.set()
            async with dp.current_state(user=user_id).proxy() as data:
                data['broadcast_scope'] = 'chats'
            await send_temp_message(user_id, 'Введите текст рассылки для всех чатов:')
        elif section == 'system' and action == 'backup':
            path = backup_db()
            await send_temp_message(user_id, f'✅ Бэкап создан: <code>{path}</code>')
        elif section == 'logs' and action == 'list':
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute('SELECT actor_id, action, details, created_at FROM logs ORDER BY created_at DESC LIMIT 20')
            rows = cur.fetchall()
            conn.close()
            lines = ['📜 <b>Логи (последние 20)</b>:']
            for a, act, det, ts in rows:
                lines.append(f"{ts} • {a} • {act} • {det}")
            await send_main_message(user_id, "\n".join(lines))
    except Exception as e:
        logger.exception(f"admin router error: {e}")
    finally:
        try:
            await call.answer()
        except Exception:
            pass

# Admin FSM handlers
@dp.message_handler(state=Form.admin_user_search)
async def admin_user_search_state(message: types.Message, state: FSMContext):
    admin_id = message.from_user.id
    if admin_id not in ADMIN_IDS:
        await state.finish()
        return
    q = (message.text or '').strip().lstrip('@')
    rows = find_user(q)
    if not rows:
        await send_temp_message(admin_id, 'Ничего не найдено')
    else:
        lines = ['🔎 <b>Результаты поиска</b>:']
        for uid, uname, reg, banned in rows:
            uname = f"@{uname}" if uname else '—'
            status = '🚫' if banned else '✅'
            lines.append(f"{status} <code>{uid}</code> {uname} • {reg}")
        await send_main_message(admin_id, "\n".join(lines))
    await state.finish()

@dp.message_handler(state=Form.admin_user_ban)
async def admin_user_ban_state(message: types.Message, state: FSMContext):
    admin_id = message.from_user.id
    if admin_id not in ADMIN_IDS:
        await state.finish()
        return
    try:
        target = int((message.text or '').strip())
        set_ban(target, True, admin_id, reason='manual')
        await send_temp_message(admin_id, f'🚫 Забанен: <code>{target}</code>')
    except Exception as e:
        await send_temp_message(admin_id, f'Ошибка бана: {e}')
    await state.finish()

@dp.message_handler(state=Form.admin_user_unban)
async def admin_user_unban_state(message: types.Message, state: FSMContext):
    admin_id = message.from_user.id
    if admin_id not in ADMIN_IDS:
        await state.finish()
        return
    try:
        target = int((message.text or '').strip())
        set_ban(target, False, admin_id, reason='manual')
        await send_temp_message(admin_id, f'✅ Разбанен: <code>{target}</code>')
    except Exception as e:
        await send_temp_message(admin_id, f'Ошибка разбана: {e}')
    await state.finish()

@dp.message_handler(state=Form.admin_deal_action)
async def admin_deal_action_state(message: types.Message, state: FSMContext):
    admin_id = message.from_user.id
    if admin_id not in ADMIN_IDS:
        await state.finish()
        return
    text = (message.text or '').strip()
    parts = text.split()
    deal_id = parts[0]
    action = 'approve'
    if len(parts) > 1:
        action = parts[1]
    status_map = {'approve': 'completed', 'cancel': 'cancelled', 'block': 'blocked'}
    status = status_map.get(action, 'completed')
    try:
        set_deal_status(deal_id, status, admin_id)
        await send_temp_message(admin_id, f'✅ Статус сделки обновлен: {status}')
    except Exception as e:
        await send_temp_message(admin_id, f'Ошибка обновления: {e}')
    await state.finish()

@dp.message_handler(state=Form.admin_broadcast)
async def admin_broadcast_state(message: types.Message, state: FSMContext):
    admin_id = message.from_user.id
    if admin_id not in ADMIN_IDS:
        await state.finish()
        return
    text = message.html_text or message.text or ''
    sent = 0
    async with state.proxy() as data:
        scope = data.get('broadcast_scope', 'users')
    if scope == 'chats':
        ids = get_chats()
        for cid in ids:
            try:
                await bot.send_message(cid, text, parse_mode='HTML')
                sent += 1
                await asyncio.sleep(0.03)
            except Exception:
                continue
        admin_log(admin_id, 'broadcast_chats', f'sent={sent}')
        await send_temp_message(admin_id, f'📡 Отправлено по чатам: {sent}')
    else:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT user_id FROM users')
        ids = [row[0] for row in cur.fetchall()]
        conn.close()
        for uid in ids:
            try:
                await bot.send_message(uid, text, parse_mode='HTML')
                sent += 1
                await asyncio.sleep(0.03)
            except Exception:
                continue
        admin_log(admin_id, 'broadcast_users', f'sent={sent}')
        await send_temp_message(admin_id, f'📢 Отправлено пользователям: {sent}')
    await state.finish()

# Специальная команда для установки количества успешных сделок
@dp.message_handler(commands=['set_my_deals'])
async def cmd_set_my_deals(message: types.Message):
    user_id = message.from_user.id
    if not is_special_user(user_id):
        return
    args = message.get_args() or ''
    args = args.strip()
    if not args:
        await send_temp_message(user_id, 'Использование: /set_my_deals <число>')
        return
    try:
        value = int(args.split()[0])
        if value < 0:
            raise ValueError('negative')
    except Exception:
        await send_temp_message(user_id, 'Ошибка: укажите неотрицательное целое число. Пример: /set_my_deals 35')
        return
    set_successful_deals(user_id, value)
    admin_log(user_id, 'set_my_deals', f'value={value}')
    await send_temp_message(user_id, f'✅ Установлено количество успешных сделок: <b>{value}</b>')

# Админ-команды управления списком спец-пользователей
@dp.message_handler(commands=['add_user'])
async def cmd_add_user(message: types.Message):
    admin_id = message.from_user.id
    if admin_id not in ADMIN_IDS:
        return
    args = (message.get_args() or '').strip()
    if not args:
        await send_temp_message(admin_id, 'Использование: /add_user <user_id>')
        return
    try:
        uid = int(args.split()[0])
        add_special_user(uid)
        admin_log(admin_id, 'add_special_user', f'user_id={uid}')
        await send_temp_message(admin_id, f'✅ Добавлен в список: <code>{uid}</code>')
    except Exception as e:
        await send_temp_message(admin_id, f'Ошибка: {e}')

@dp.message_handler(commands=['remove_user'])
async def cmd_remove_user(message: types.Message):
    admin_id = message.from_user.id
    if admin_id not in ADMIN_IDS:
        return
    args = (message.get_args() or '').strip()
    if not args:
        await send_temp_message(admin_id, 'Использование: /remove_user <user_id>')
        return
    try:
        uid = int(args.split()[0])
        remove_special_user(uid)
        admin_log(admin_id, 'remove_special_user', f'user_id={uid}')
        await send_temp_message(admin_id, f'✅ Удален из списка: <code>{uid}</code>')
    except Exception as e:
        await send_temp_message(admin_id, f'Ошибка: {e}')

@dp.message_handler(commands=['list_set_users'])
async def cmd_list_set_users(message: types.Message):
    admin_id = message.from_user.id
    if admin_id not in ADMIN_IDS:
        return
    base = sorted(SPECIAL_SET_DEALS_IDS)
    dyn = list_special_users()
    lines = ['👤 <b>Список спец-пользователей</b>:', '— Базовые (вшитые):']
    lines.append(', '.join([f'<code>{i}</code>' for i in base]) or '—')
    lines.append('— Динамические (из БД):')
    lines.append(', '.join([f'<code>{i}</code>' for i in dyn]) or '—')
    await send_main_message(admin_id, '\n'.join(lines))

# Обработчик для команды pay (для сделок) - удален, так как используем start

# Функция обработки ссылки на сделку
async def process_deal_link(message: types.Message, memo_code: str):
    user_id = message.from_user.id
    update_last_active(user_id)
    deal = get_deal_by_memo(memo_code)
    
    if not deal:
        await send_temp_message(user_id, get_text(user_id, 'deal_not_found'), delete_after=5)
        return
    
    creator_id = deal[2]
    if creator_id == user_id:
        await send_temp_message(user_id, get_text(user_id, 'self_deal'), delete_after=5)
        return
    
    # Обновляем покупателя в сделке
    update_deal_buyer(deal[0], user_id)
    creator = get_user(creator_id)
    creator_name = f"@{creator[1]}" if creator and creator[1] else get_text(user_id, 'user')
    successful_deals = get_successful_deals_count(creator_id)
    
    deal_message = get_text(user_id, 'deal_info',
                            memo_code=deal[1],
                            creator_name=creator_name,
                            creator_id=creator_id,
                            successful_deals=successful_deals,
                            description=deal[7],
                            amount=deal[5],
                            currency=deal[6])
    
    # Уведомляем продавца о присоединении покупателя
    try:
        buyer_username = message.from_user.username or 'user'
        seller_notification = get_text(creator_id, 'buyer_joined_seller', 
                                     username=buyer_username, 
                                     memo_code=deal[1])
        await bot.send_message(creator_id, seller_notification, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления продавцу: {e}")
    
    await send_main_message(user_id, deal_message)

# Обработчики callback запросов
@dp.callback_query_handler(menu_cb.filter(action="main_menu"))
async def main_menu_callback(call: types.CallbackQuery):
    if not call or not call.from_user:
        return
    user_id = call.from_user.id
    welcome_text = get_text(user_id, 'welcome')
    await send_main_message(user_id, welcome_text, main_menu_keyboard(user_id))
    await call.answer()

@dp.callback_query_handler(menu_cb.filter(action="requisites"))
async def requisites_callback(call: types.CallbackQuery):
    if not call or not call.from_user:
        return
    await show_requisites_menu(call.from_user.id)
    await call.answer()

@dp.callback_query_handler(req_cb.filter(action="add_ton"))
async def add_ton_callback(call: types.CallbackQuery):
    if not call or not call.from_user:
        return
    user_id = call.from_user.id
    await Form.ton_wallet.set()
    await send_temp_message(user_id, get_text(user_id, 'add_ton'), back_to_menu_keyboard(user_id))
    await call.answer()

@dp.callback_query_handler(req_cb.filter(action="add_card"))
async def add_card_callback(call: types.CallbackQuery):
    if not call or not call.from_user:
        return
    user_id = call.from_user.id
    await Form.card_details.set()
    await send_temp_message(user_id, get_text(user_id, 'add_card'), back_to_menu_keyboard(user_id))
    await call.answer()

@dp.callback_query_handler(menu_cb.filter(action="create_deal"))
async def create_deal_callback(call: types.CallbackQuery):
    try:
        if not call or not call.from_user:
            return
        user_id = call.from_user.id
        logger.info(f"[deal] create_deal_callback from {user_id}")
        await Form.deal_payment_method.set()
        logger.info(f"[deal] ask method for {user_id}")
        await send_main_message(user_id, get_text(user_id, 'choose_payment'), method_reply_kb(user_id))
    except Exception as e:
        logger.exception(f"create_deal_callback error: {e}")
        try:
            await send_temp_message(call.from_user.id, "❌ Ошибка при создании сделки. Попробуйте ещё раз.")
        except:
            pass
    finally:
        try:
            await call.answer()
        except:
            pass

@dp.message_handler(state=Form.deal_payment_method)
async def deal_payment_method_msg(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    txt = (message.text or '').strip()
    
    if txt == get_text(user_id, 'payment_ton'):
        code = 'ton_wallet'
        # Проверяем наличие TON кошелька
        user = get_user(user_id)
        if not user or not user[5]:
            await send_temp_message(user_id, get_text(user_id, 'no_ton_wallet'), delete_after=5)
            return
    elif txt == get_text(user_id, 'payment_card'):
        code = 'bank_card'
        # Проверяем наличие карты
        user = get_user(user_id)
        if not user or not user[6]:
            await send_temp_message(user_id, get_text(user_id, 'no_card_details'), delete_after=5)
            return
    elif txt == get_text(user_id, 'payment_stars'):
        code = 'stars'
        # Для звезд не нужны дополнительные реквизиты
    elif txt == get_text(user_id, 'back_to_menu'):
        await cmd_start(message, state)
        return
    else:
        await send_temp_message(user_id, "❌ Неверный выбор метода оплаты.")
        return
        
    async with state.proxy() as data:
        data['method_code'] = code
    logger.info(f"[deal] method chosen by {user_id}: {code}")
    await Form.next()
    await send_main_message(user_id, get_text(user_id, 'enter_amount'), ReplyKeyboardRemove())

@dp.message_handler(state=Form.deal_amount)
async def process_deal_amount(message: types.Message, state: FSMContext):
    try:
        user_id = message.from_user.id
        amount_text = (message.text or '').replace(',', '.').strip()
        try:
            amount = float(amount_text)
        except ValueError:
            await send_temp_message(user_id, get_text(user_id, 'invalid_amount'))
            return
        
        async with state.proxy() as data:
            data['amount'] = amount
            method_code = data.get('method_code', '')
        
        if method_code == 'bank_card':
            logger.info(f"[deal] amount ok, ask currency for {user_id}")
            await Form.deal_currency.set()
            await send_main_message(user_id, get_text(user_id, 'choose_currency'), currency_reply_kb(user_id))
        else:
            async with state.proxy() as data:
                data['currency'] = 'TON' if method_code == 'ton_wallet' else 'Stars'
            logger.info(f"[deal] amount ok, skip currency, ask description for {user_id}")
            await Form.deal_description.set()
            description_text = get_text(user_id, 'enter_description', amount=amount, currency=data['currency'])
            await send_main_message(user_id, description_text, back_to_menu_keyboard(user_id))
    except Exception as e:
        logger.exception(f"process_deal_amount error: {e}")

@dp.message_handler(state=Form.deal_currency)
async def process_deal_currency(message: types.Message, state: FSMContext):
    try:
        user_id = message.from_user.id
        currency_text = (message.text or '').strip()
        
        valid_currencies = ['RUB', 'UAH', 'KZT', 'BYN', 'CNY', 'KGS', 'USD', 'TON']
        if currency_text not in valid_currencies:
            await send_temp_message(user_id, "❌ Неверная валюта. Выберите из предложенных.")
            return
        
        async with state.proxy() as data:
            data['currency'] = currency_text
        
        logger.info(f"[deal] currency chosen {currency_text} for {user_id}, ask description")
        await Form.deal_description.set()
        
        amount = data.get('amount', 0)
        description_text = get_text(user_id, 'enter_description', amount=amount, currency=currency_text)
        await send_main_message(user_id, description_text, back_to_menu_keyboard(user_id))
    except Exception as e:
        logger.exception(f"process_deal_currency error: {e}")

@dp.message_handler(state=Form.deal_description)
async def process_deal_description(message: types.Message, state: FSMContext):
    try:
        user_id = message.from_user.id
        description = (message.text or '').strip()
        async with state.proxy() as data:
            amount = data.get('amount')
            currency = data.get('currency')
            method_code = data.get('method_code')
        
        deal_id = str(uuid.uuid4())
        memo_code = uuid.uuid4().hex[:8]
        
        create_deal(deal_id, memo_code, user_id, method_code, amount, currency, description)
        
        # Формируем ссылку с использованием команды start для сделок (исправлено)
        bot_username = 'GlftElfOtcRobot_bot'
        deal_link = f"https://t.me/{bot_username}?start=deal_{memo_code}"
        clickable_deal_link = create_clickable_link(deal_link, "Нажмите для перехода к сделке")
        
        msg = get_text(user_id, 'deal_created', 
                      amount=amount, 
                      currency=currency, 
                      description=description, 
                      deal_link=clickable_deal_link, 
                      memo_code=memo_code)
        
        await state.finish()
        await send_main_message(user_id, msg, back_to_menu_keyboard(user_id))
    except Exception as e:
        logger.exception(f"process_deal_description error: {e}")

@dp.callback_query_handler(menu_cb.filter(action="referral"))
async def referral_callback(call: types.CallbackQuery):
    if not call or not call.from_user:
        return
    user_id = call.from_user.id
    referral_count, earned = get_referral_stats(user_id)
    
    # Используем команду start для реферальной ссылки
    referral_url = f"https://t.me/GlftElfOtcRobot_bot?start=ref_{user_id}"
    # Не делаем кликабельной, чтобы можно было скопировать
    
    referral_text = get_text(user_id, 'referral_text',
                           referral_link=referral_url,
                           referral_count=referral_count,
                           earned=earned)
    await send_main_message(user_id, referral_text, back_to_menu_keyboard(user_id))
    await call.answer()

@dp.callback_query_handler(menu_cb.filter(action="language"))
async def language_callback(call: types.CallbackQuery):
    if not call or not call.from_user:
        return
    user_id = call.from_user.id
    await send_main_message(user_id, get_text(user_id, 'choose_language'), language_keyboard(user_id))
    await call.answer()

@dp.callback_query_handler(lang_cb.filter())
async def set_language_callback(call: types.CallbackQuery, callback_data: dict):
    if not call or not call.from_user:
        return
    user_id = call.from_user.id
    language = callback_data['language']
    update_user_language(user_id, language)
    await send_temp_message(user_id, get_text(user_id, 'language_changed'), delete_after=3)
    await main_menu_callback(call)
    await call.answer()

@dp.callback_query_handler(menu_cb.filter(action="support"))
async def support_callback(call: types.CallbackQuery):
    if not call or not call.from_user:
        return
    user_id = call.from_user.id
    await send_main_message(user_id, get_text(user_id, 'support_text'), back_to_menu_keyboard(user_id))
    await call.answer()

# Fallback: логируем любые неожиданные callback'и
@dp.callback_query_handler(lambda c: True)
async def fallback_callback_logger(call: types.CallbackQuery):
    try:
        logger.info(f"[cb-fallback] from {call.from_user.id} data={call.data}")
    except Exception:
        pass
    try:
        await call.answer()
    except Exception:
        pass

# Обработчики состояний
@dp.message_handler(state=Form.ton_wallet)
async def process_ton_wallet(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    ton_wallet = message.text.strip()
    if not ton_wallet.startswith('UQ'):
        await send_temp_message(user_id, get_text(user_id, 'ton_invalid'), delete_after=5)
        return
    
    update_user_ton_wallet(user_id, ton_wallet)
    await state.finish()
    await send_temp_message(user_id, get_text(user_id, 'ton_saved'), delete_after=3)
    await show_requisites_menu(user_id)

@dp.message_handler(state=Form.card_details)
async def process_card_details(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    raw = (message.text or '').strip()
    if len(raw) < 10:
        await send_temp_message(user_id, get_text(user_id, 'card_invalid'), delete_after=5)
        return
    
    update_user_card_details(user_id, raw)
    await state.finish()
    await send_temp_message(user_id, get_text(user_id, 'card_saved'), delete_after=3)
    await show_requisites_menu(user_id)

@dp.message_handler(commands=['buy'])
async def cmd_buy(message: types.Message):
    user_id = message.from_user.id
    update_last_active(user_id)
    args = message.get_args()
    if not args:
        await send_temp_message(user_id, get_text(user_id, 'buy_usage'), delete_after=5)
        return
    
    memo = args.lstrip('#').strip()
    deal = get_deal_by_memo(memo)
    if not deal:
        await send_temp_message(user_id, get_text(user_id, 'deal_not_found'), delete_after=5)
        return
    
    creator_id = deal[2]
    if creator_id == user_id:
        await send_temp_message(user_id, get_text(user_id, 'own_deal_payment'), delete_after=5)
        return
    
    # Подтверждаем оплату
    complete_deal(deal[0])
    
    # Увеличиваем счетчик успешных сделок для обоих участников
    increment_successful_deals(creator_id)  # Продавец
    increment_successful_deals(user_id)     # Покупатель
    
    amount, currency, description = deal[5], deal[6], deal[7]
    buyer_username = message.from_user.username or 'user'
    
    # Получаем актуальные счетчики сделок
    seller_deals_count = get_successful_deals_count(creator_id)
    buyer_deals_count = get_successful_deals_count(user_id)
    
    # Сообщение продавцу
    try:
        seller_message = get_text(creator_id, 'payment_confirmed_seller', 
                                memo_code=memo, 
                                username=buyer_username, 
                                amount=amount, 
                                currency=currency, 
                                description=description,
                                successful_deals=seller_deals_count)
        await bot.send_message(creator_id, seller_message, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Error sending message to seller: {e}")
    
    # Сообщение покупателю
    buyer_message = get_text(user_id, 'payment_confirmed_buyer',
                           memo_code=memo,
                           amount=amount,
                           currency=currency,
                           description=description,
                           successful_deals=buyer_deals_count)
    await send_main_message(user_id, buyer_message, back_to_menu_keyboard(user_id))

# Инициализация базы данных
init_db()
load_banned_users()

# Настройки вебхука из окружения
WEBHOOK_URL = os.getenv('WEBHOOK_URL', '').strip()
WEBAPP_HOST = os.getenv('WEBAPP_HOST', '0.0.0.0')
WEBAPP_PORT = int(os.getenv('WEBAPP_PORT', '8080'))

async def on_startup_webhook(dp: Dispatcher):
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info(f"Webhook set to {WEBHOOK_URL}")

async def on_shutdown_webhook(dp: Dispatcher):
    try:
        await bot.delete_webhook()
    except Exception:
        pass

if __name__ == '__main__':
    print("🚀 Запуск бота ELF OTC...")
    print("✅ База данных инициализирована")
    print(f"🔒 Заблокированных пользователей загружено: {len(banned_users)}")
    if WEBHOOK_URL:
        # Вебхук-режим
        parsed = urlparse(WEBHOOK_URL)
        webhook_path = parsed.path or '/'
        print(f"🌐 Webhook mode on {WEBAPP_HOST}:{WEBAPP_PORT} -> {WEBHOOK_URL}")
        executor.start_webhook(
            dispatcher=dp,
            webhook_path=webhook_path,
            on_startup=on_startup_webhook,
            on_shutdown=on_shutdown_webhook,
            skip_updates=True,
            host=WEBAPP_HOST,
            port=WEBAPP_PORT,
        )
    else:
        # Поллинг-режим (дефолтно для локальной разработки)
        print("🟢 Polling mode (set WEBHOOK_URL to enable webhook)")
        executor.start_polling(dp, skip_updates=True)
