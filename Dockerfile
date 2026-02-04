FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY tests/ ./tests/
COPY config/ ./config/
COPY data/ ./data/

ENV PYTHONPATH=/app

CMD ["python", "main.py"]
