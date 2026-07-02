FROM python:3.11-slim

WORKDIR /app

COPY services/carbon-forecaster/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/shared /app/shared
COPY services/carbon-forecaster/forecaster.py .

CMD ["python", "forecaster.py"]
