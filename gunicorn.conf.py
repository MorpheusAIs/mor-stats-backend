# Gunicorn configuration file
import multiprocessing
import os

# Server socket
bind = "0.0.0.0:8000"
backlog = 2048

# Worker processes - use WEB_CONCURRENCY env var, or calculate based on CPU cores
workers = int(os.getenv('WEB_CONCURRENCY', multiprocessing.cpu_count() * 2 + 1))
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000

# Timeout settings
timeout = 120  # reduced from 600
keepalive = 2  # reduced from 5
graceful_timeout = 120

# Logging
errorlog = "-"
loglevel = "info"  # changed from debug
accesslog = "-"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

# Restart workers after this many requests
max_requests = 1000
max_requests_jitter = 50

# Misc
capture_output = True
enable_stdio_inheritance = True 