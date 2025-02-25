import time
import threading
from collections import deque
from functools import wraps

# A deque to hold timestamps for each API call.
api_call_timestamps = deque()
lock = threading.Lock()

def record_api_call():
    """Record an API call by adding its timestamp and purge old entries."""
    now = time.time()
    with lock:
        api_call_timestamps.append(now)
        # Remove timestamps older than 10 seconds.
        while api_call_timestamps and (now - api_call_timestamps[0] > 10):
            api_call_timestamps.popleft()

def get_api_usage():
    """Return the number of API calls in the last 10 seconds."""
    now = time.time()
    with lock:
        while api_call_timestamps and (now - api_call_timestamps[0] > 10):
            api_call_timestamps.popleft()
        return len(api_call_timestamps)

def track_api_usage(func):
    """Decorator to track API usage."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        record_api_call()
        return func(*args, **kwargs)
    return wrapper