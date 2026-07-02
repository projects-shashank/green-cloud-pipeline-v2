FROM python:3.11-slim

WORKDIR /app

COPY services/job-generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/shared /app/shared
COPY services/job-generator/generator.py .

CMD ["python", "generator.py"]
