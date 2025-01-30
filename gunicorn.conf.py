# Gunicorn configuration file
import multiprocessing
import os

# Server socket
bind = "0.0.0.0:8000"
backlog = 2048

# Worker processes
workers = int(os.getenv('WEB_CONCURRENCY', '2'))  # Fixed number instead of CPU-based
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000

# Timeout settings
timeout = 30  # Reduced from 120
graceful_timeout = 30  # Reduced from 120
keepalive = 2

# Logging
errorlog = "-"
loglevel = "debug"  # Changed to debug to see more info
accesslog = "-"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

# Restart workers after this many requests
max_requests = 1000
max_requests_jitter = 50

# Docker specific settings
preload_app = False  # Prevent memory issues
worker_tmp_dir = "/dev/shm"  # Use shared memory for temp files
forwarded_allow_ips = "*"  # Trust X-Forwarded-* headers

# Graceful shutdown settings
check_config = True
capture_output = True
enable_stdio_inheritance = True

# Signal handling
reload_engine = "auto"
reload_extra_files = [] 