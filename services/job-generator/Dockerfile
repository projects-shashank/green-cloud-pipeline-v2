FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared library
COPY ../shared /app/shared

# Copy service code
COPY generator.py .

CMD ["python", "generator.py"]
