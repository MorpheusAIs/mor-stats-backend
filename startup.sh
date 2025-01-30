#!/bin/bash

# Create runtime directories
mkdir -p /dev/shm/gunicorn
chmod 777 /dev/shm/gunicorn

# Start Gunicorn with proper signal handling
cd /home/site/wwwroot && exec gunicorn main:app \
    --preload \
    --worker-tmp-dir /dev/shm/gunicorn \
    --worker-class uvicorn.workers.UvicornWorker \
    --workers 2 \
    --bind 0.0.0.0:8000 