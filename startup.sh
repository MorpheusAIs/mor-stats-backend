#!/bin/bash

# Create runtime directories
mkdir -p /dev/shm/gunicorn
chmod 777 /dev/shm/gunicorn

# Ensure the sheets_config directory exists in the correct location
mkdir -p /home/site/wwwroot/sheets_config

# Create the credentials file from environment variable
echo "$GOOGLE_SHEETS_CREDENTIALS" > /home/site/wwwroot/sheets_config/credentials.json
chmod 644 /home/site/wwwroot/sheets_config/credentials.json

# Debug: Print file contents and permissions
ls -la /home/site/wwwroot/sheets_config/
cat /home/site/wwwroot/sheets_config/credentials.json

# Set environment variables for Gunicorn
export GUNICORN_CMD_ARGS="--config=gunicorn.conf.py"
export PYTHONUNBUFFERED=1

# Start Gunicorn with proper signal handling
cd /home/site/wwwroot && exec gunicorn main:app \
    --preload \
    --worker-tmp-dir /dev/shm/gunicorn \
    --worker-class uvicorn.workers.UvicornWorker \
    --workers 2 \
    --bind 0.0.0.0:8000 