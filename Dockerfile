
FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Set workdir
WORKDIR /app

# System deps (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt /app/requirements.txtFROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt


RUN python -m pip install -U pip setuptools wheel && \
    pip install --no-cache-dir --only-binary=:all: -r requirements.txt

COPY . /app

EXPOSE 8080

ENV WEBAPP_HOST=0.0.0.0 \
    WEBAPP_PORT=8080 \
    WEBHOOK_URL=""

CMD ["python", "anonim.py"]
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . /app

# Expose port for webhook
EXPOSE 8080

# Default envs (override in hosting platform)
ENV WEBAPP_HOST=0.0.0.0 \
    WEBAPP_PORT=8080 \
    WEBHOOK_URL=""

# Start the bot (webhook mode if WEBHOOK_URL set, otherwise polling)
CMD ["python", "ELF.py"]
