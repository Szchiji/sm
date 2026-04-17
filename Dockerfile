FROM python:3.11-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制代码
COPY bot.py .

# Railway 会分配 PORT 环境变量
EXPOSE 8080

CMD ["python", "bot.py"]
