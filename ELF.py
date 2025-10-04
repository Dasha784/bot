from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import time
import os

from collections import defaultdict


API_TOKEN =("8466659548:AAE2Jn934ocnvTE2SwtkN0MvfnSRHOSrlBQ")

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

user_interests = {}
waiting = []
pairs = {}
dislikes = defaultdict(lambda: defaultdict(list))
reports = defaultdict(lambda: defaultdict(list))

INTERESTS = {
    "games": "Ігри",
    "communication": "Спілкування",
    "books": "Книги",
    "movies": "Фільми",
    "anime": "Аніме",
    "travel": "Подорожі",
    "sports": "Спорт",
    "IT":"IT",
    "mems":"Меми",
    "tips":"Поради",
    
}

text = {
    "choose_lang":
        "Ми намагаємося знайти для вас "
        "\nспіврозмовника ,який вибрав"
        "\nсхожі інтереси ."
        "\n"
        "\n"


        "\nВиберіть ваші інтереси:",

    
    "done": "Інтереси збережено! Тепер напиши /search для пошуку співрозмовника.",
    "searching": "Пошук співрозмовника...",
    "found": "Співрозмовника знайдено! Можеш почати спілкування",
    "stopped": "Діалог завершено. Напиши /search для пошуку нового співрозмовника",
     "rules":"З правилами можна ознайомитися тут https://t.me/anonchatt2_bot_rules ",

}

REPORT_REASONS = [
    "Реклама",
    "Продаж",
    "Насилля",
    "Пропаганда суїциду",
    "Спам",
    "Спілкується не за тим інтересом",
    "Розпалювання ворожнечі"
]


def feedback_keyboard():
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(
        InlineKeyboardButton("👍🏻", callback_data="feedback_like"),
        InlineKeyboardButton("👎🏻", callback_data="feedback_dislike"),
        InlineKeyboardButton("🚫", callback_data="feedback_report"),
    )
    return kb


def report_reasons_keyboard():
    kb = InlineKeyboardMarkup(row_width=2)
    for reason in REPORT_REASONS:
        kb.add(InlineKeyboardButton(reason, callback_data=f"report_{reason}"))
    return kb


def check_punishment(user_id, action_type="dislike", reason=None):
    now = time.time()
    punishments = [
        (1, 3600),
        (2, 7200),
        (3, 3600),
        (4, 86400),
        (5, 172800),
        (6, 259200),
        (7, 604800),
        (8, 1209600),
        (9, 1814400),
        (10, 2592000),
        (11, 5184000),
        ("next", 777600)
    ]

    if action_type == "dislike":
        dislikes[user_id][reason].append(now)
        recent = [t for t in dislikes[user_id][reason] if now - t < 3600]
        if len(recent) >= 10:
            count = len(recent)
            return punishments[min(count, len(punishments)) - 1][1]

    elif action_type == "report":
        reports[user_id][reason].append(now)
        recent = [t for t in reports[user_id][reason] if now - t < 3600]
        if len(recent) > 5:
            count = len(recent)
            return punishments[min(count, len(punishments)) - 1][1]

    return None
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def main_menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(
        KeyboardButton("Почати діалог"),
        KeyboardButton("Інтереси"),
        KeyboardButton("Правила чату")
    )
    return kb



@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await show_interests_menu(message)
    


async def show_interests_menu(message: types.Message):
    kb = await build_interest_kb(message.from_user.id)
    await message.answer(text["choose_lang"], reply_markup=kb)


async def build_interest_kb(uid):
    kb = InlineKeyboardMarkup(row_width=2)
    for key, val in INTERESTS.items():
        selected = "✅" if key in user_interests.get(uid, set()) else ""
        kb.insert(InlineKeyboardButton(val + selected, callback_data=f"int_{key}"))
    kb.add(InlineKeyboardButton("✅Готово", callback_data="done"))
    return kb


@dp.callback_query_handler(lambda c: c.data.startswith("int_"))
async def toggle_interest(callback: types.CallbackQuery):
    uid = callback.from_user.id
    key = callback.data.replace("int_", "")
    user_interests.setdefault(uid, set())
    if key in user_interests[uid]:
        user_interests[uid].remove(key)
    else:
        user_interests[uid].add(key)
    await callback.message.edit_reply_markup(await build_interest_kb(uid))
    await callback.answer("✅Вибір оновлено")


@dp.callback_query_handler(lambda c: c.data == "done")
async def finish_interests(callback: types.CallbackQuery):
    await callback.message.answer(text["done"])


@dp.message_handler(commands=["search"])
async def cmd_search(message: types.Message):
    uid = message.from_user.id
    if uid not in waiting:
        waiting.append(uid)

    for other in waiting:
        if other != uid:
            if user_interests.get(uid) & user_interests.get(other):
                waiting.remove(uid)
                waiting.remove(other)
                pairs[uid] = other
                pairs[other] = uid
                await message.answer(text["found"])
                await bot.send_message(other, text["found"])
                break
@dp.message_handler(commands=["rules"])
async def cmd_rules(message: types.Message):
    await message.answer(text["rules"])


@dp.message_handler(commands=["stop"])
async def cmd_stop(message: types.Message):
    uid = message.from_user.id

    if uid in pairs:
        partner = pairs[uid]
        del pairs[uid]
        if partner in pairs:
            del pairs[partner]
            await bot.send_message(partner, text["stopped"], reply_markup=feedback_keyboard())


@dp.callback_query_handler(lambda c: c.data.startswith("feedback_"))
async def process_feedback(callback: types.CallbackQuery):
    uid = callback.from_user.id
    data = callback.data
    if data == "feedback_like":
        await callback.answer("Дякуємо за оцінку, можете продовжити спілкуватися!")
    elif data == "feedback_dislike":
        punishment = check_punishment(uid, "dislike", "general")
        if punishment:
            await callback.answer(f"Ви порушили наші правила. Бан на {punishment // 3600} год.")
        else:
            await callback.answer("Дякуємо за оцінку, можете продовжити спілкуватися!")
    elif data == "feedback_report":
        await callback.message.answer("Оберіть причину:", reply_markup=report_reasons_keyboard())
        await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("report_"))
async def process_report(callback: types.CallbackQuery):
    uid = callback.from_user.id
    reason = callback.data.replace("report_", "")
    punishment = check_punishment(uid, "report", reason)
    if punishment:
        await callback.answer(f"Скарга зарахована. Бан на {punishment // 3600} год.")
    else:
        await callback.answer("Скарга прийнята")
@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    help_text = (
        "Це бот для анонімного спілкування "
        "\nв Телеграмі"
        "\n"
        "\nБот вміє пересилати повідомлення,"
        "\nвідео,гіфки,стікери,"
        "\nаудіоповідомлееня та"
        "\nвідеоповідомлення."
        "\n"
        
        "\n"
        "📌 Команди бота:\n"
        
        "/start – Розпочати роботу з ботом\n"
        "/search – Знайти співрозмовника\n"
        "/stop – Завершити діалог\n"
        "/help – Список команд та їх опис\n"
        "/rules – Правила чату\n"
    )
    await message.answer(help_text)
@dp.message_handler(lambda message: message.text == "Почати діалог")
async def start_dialog(message: types.Message):
    await cmd_search(message)


@dp.message_handler(lambda message: message.text == "Інтереси")
async def interests(message: types.Message):
    await show_interests_menu(message)


@dp.message_handler(lambda message: message.text == "Правила чату")
async def rules_btn(message: types.Message):
    await message.answer(text["rules"])




@dp.message_handler()
async def relay(message: types.Message):
    uid = message.from_user.id
    if uid in pairs:
        partner = pairs[uid]
        await bot.send_message(partner, message.text)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
