FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libeccodes-dev \
    libeccodes-tools \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/data/raw /app/data/processed /app/logs

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["sleep", "infinity"]