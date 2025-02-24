import logging
from collections import defaultdict, deque
import io
from rich.text import Text
from rich.console import Console

# A shared dictionary to store logs per bot.
log_buffers = defaultdict(lambda: deque(maxlen=1000))

class CustomFormatter(logging.Formatter):
    def formatException(self, exc_info):
        if exc_info is None:
            return ""
        return super().formatException(exc_info)
    
    def format(self, record):
        s = super().format(record)
        if "NoneType: None" in s:
            s = s.replace("NoneType: None", "").strip()
        return s

class InMemoryLogHandler(logging.Handler):
    def emit(self, record):
        try:
            # Format the record using our custom formatter.
            msg = self.format(record)
            # Convert the Rich markup to a Rich Text object.
            text_obj = Text.from_markup(msg)
            # Use a StringIO buffer and a temporary Console to capture ANSI-coded output.
            buffer = io.StringIO()
            console = Console(file=buffer, force_terminal=True, width=80)
            console.print(text_obj)
            ansi_msg = buffer.getvalue()
            # Retrieve the bot_slug (default "global")
            bot_slug = getattr(record, "bot_slug", "global")
            log_buffers[bot_slug].append(ansi_msg)
        except Exception:
            self.handleError(record)
