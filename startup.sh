#!/bin/bash

# Create runtime directories
mkdir -p /dev/shm/gunicorn
chmod 777 /dev/shm/gunicorn

# Set environment variables for Gunicorn
export GUNICORN_CMD_ARGS="--config=gunicorn.conf.py"
export PYTHONUNBUFFERED=1
export PYTHONPATH="${PYTHONPATH}:/tmp/*/antenv/lib/python3.11/site-packages"

# Start Gunicorn with proper signal handling
exec gunicorn main:app \
    --preload \
    --worker-tmp-dir /dev/shm/gunicorn \
    --worker-class uvicorn.workers.UvicornWorker \
    --workers 2 \
    --bind 0.0.0.0:8000 