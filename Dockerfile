# 1. Base Image (Official Python 3.10 Slim)
FROM python:3.10-slim

# 2. Set Working Directory
WORKDIR /app

# 3. Install System Dependencies (Needed for Prophet/DuckDB on Linux)
RUN apt-get update && apt-get install -y \
    build-essential \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# 4. Copy Requirements & Install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy Source Code & Config
COPY src/ ./src/
COPY config.yaml .

# 6. Create Directories for Data & Logs
RUN mkdir -p data mlruns cache

# 7. Default Command
CMD ["python", "src/run_pipeline3.py"]