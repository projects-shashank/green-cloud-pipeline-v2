FROM python:3.11-slim

WORKDIR /app

COPY services/admission-controller/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/shared /app/shared
COPY services/admission-controller/controller.py .

CMD ["python", "controller.py"]
