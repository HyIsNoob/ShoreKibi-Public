# Sử dụng image Python slim (có sẵn Python 3.9)
FROM python:3.9-slim

# Cập nhật và cài đặt FFmpeg
RUN apt-get update && apt-get install -y ffmpeg

# Thiết lập thư mục làm việc
WORKDIR /app

# Copy file requirements.txt và cài đặt các thư viện cần thiết
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy toàn bộ mã nguồn vào container
COPY . .

# Chạy file chính của bot
CMD ["python", "main.py"]
