# 1. Base Image (Official Python)
FROM python:3.10-slim

# 2. Set Working Directory
WORKDIR /app

# 3. Install System Dependencies (if needed)
# RUN apt-get update && apt-get install -y gcc

# 4. Copy Requirements & Install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy Source Code
COPY src/ ./src/
COPY config.yaml .

# 6. Create Data & Cache Folders
RUN mkdir data cache

# 7. Default Command
CMD ["python", "src/run_pipeline.py"]