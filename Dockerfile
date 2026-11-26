FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .

# 机器人不需要 root 权限运行。
RUN useradd --create-home --uid 10001 botuser \
    && chown -R botuser:botuser /app
USER botuser

EXPOSE 8080

CMD ["python", "bot.py"]
