FROM python:3.11-slim

WORKDIR /app

COPY services/worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/shared /app/shared
COPY services/worker/worker.py .

CMD ["python", "worker.py"]
