FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy source code
COPY . /app

# Install dependencies
RUN python3 -m pip  install --no-cache-dir -r requirements.txt

# Run the script
CMD ["python", "main.py"]