#!/bin/bash
cd /home/site/wwwroot
source antenv/bin/activate
gunicorn --bind=0.0.0.0:8000 --timeout 600 --access-logfile '-' --error-logfile '-' --workers 4 -k uvicorn.workers.UvicornWorker main:app 