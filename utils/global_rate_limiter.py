import time
import threading

class GlobalRateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.lock = threading.Lock()
        self.call_times = []

    def wait_for_slot(self):
        with self.lock:
            now = time.time()
            # Remove timestamps older than the period
            self.call_times = [t for t in self.call_times if now - t < self.period]
            if len(self.call_times) >= self.max_calls:
                # Wait until the oldest call is outside the period
                sleep_time = self.period - (now - self.call_times[0])
                time.sleep(sleep_time)
            self.call_times.append(time.time())

# Create a global rate limiter instance
global_rate_limiter = GlobalRateLimiter(max_calls=80, period=10)