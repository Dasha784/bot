# Use Python 3.11 slim image
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
COPY requirements.txt /app/requirements.txt
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
