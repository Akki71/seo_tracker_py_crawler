FROM python:3.11-slim

# System deps for psycopg2, Pillow, ReportLab, lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    libjpeg-dev \
    zlib1g-dev \
    libfreetype6-dev \
    libssl-dev \
    fonts-dejavu-core \
    fonts-dejavu-extra \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create output directory for Excel/PDF files
RUN mkdir -p output screenshots

# Expose FastAPI port
EXPOSE 8000

# Startup: init DB schema, then serve
CMD ["python", "startup.py"]
