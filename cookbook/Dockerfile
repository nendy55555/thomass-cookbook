FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Database stored on persistent volume
ENV COOKBOOK_DB_DIR=/data

EXPOSE 8742

CMD ["python3", "server.py"]
