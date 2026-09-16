FROM python:3.10-slim

ARG HTTP_PROXY
ARG HTTPS_PROXY
ENV NO_PROXY=app,localhost,127.0.0.1
ENV PYTHONUNBUFFERED=1

# Instala Chromium e dependências do sistema
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Variáveis do Chromium no Linux
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

CMD ["gunicorn", "--worker-class", "gthread", "--threads", "4", "-w", "1", "-b", "0.0.0.0:5000", "--access-logfile", "-", "--error-logfile", "-", "app:app"]