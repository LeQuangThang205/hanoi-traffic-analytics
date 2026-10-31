# Hanoi Traffic Analytics - runtime image
# Python 3.11 + OpenJDK 17 de chay PySpark local mode.
FROM python:3.11-slim-bookworm

# Java cho PySpark; procps/curl cho tien ich container va healthcheck.
# tzdata bat buoc de zoneinfo doc duoc "Asia/Ho_Chi_Minh" (collector).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Cai dependency truoc de tan dung Docker layer cache.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy toan bo ma nguon vao /app.
COPY . .

EXPOSE 8501

# Lenh mac dinh (docker-compose se override cho tung service).
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
