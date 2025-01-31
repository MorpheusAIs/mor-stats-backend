#!/bin/bash

# Create runtime directories
mkdir -p /dev/shm/gunicorn
chmod 777 /dev/shm/gunicorn

# Debug mount information
echo "Checking mount points..."
mount
echo "Listing /config directory..."
ls -la /config
echo "Current working directory..."
pwd
echo "Current directory contents..."
ls -la

# Check mount directory
if [ ! -d "/config" ]; then
    echo "Warning: Mount directory /config does not exist"
    mkdir -p /config
fi

# Check credentials file
if [ ! -f "/config/credentials.json" ]; then
    echo "Warning: credentials.json not found in mount"
fi

# Start Gunicorn with proper signal handling
cd /home/site/wwwroot && exec gunicorn main:app \
    --preload \
    --worker-tmp-dir /dev/shm/gunicorn \
    --worker-class uvicorn.workers.UvicornWorker \
    --workers 2 \
    --bind 0.0.0.0:8000 