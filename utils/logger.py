# utils/logger.py
import os
import datetime
import logging

# Create a new log folder with the current date & time.
current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FOLDER = f"logs_polyBot_{current_time}"
os.makedirs(LOG_FOLDER, exist_ok=True)

def sanitize_filename(filename: str) -> str:
    """Replace invalid filename characters with underscores."""
    return "".join(c if c.isalnum() or c in (' ', '_') else '_' for c in filename).strip().replace(" ", "_")

def get_market_logger(market: dict) -> logging.Logger:
    """Creates (or returns) a logger for a specific market.
    
    Each market logs to its own file in the folder defined by LOG_FOLDER.
    """
    market_question = market.get('market_slug', 'unknown_market')
    sanitized_question = sanitize_filename(market_question)
    file_name = f"{sanitized_question}.txt"
    file_path = os.path.join(LOG_FOLDER, file_name)

    # Create a logger with a unique name.
    logger_name = f"market_{market.get('condition_id', sanitized_question)}"
    logger = logging.getLogger(logger_name)
    
    # Avoid adding multiple handlers if the logger is already set up.
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(file_path)
        formatter = logging.Formatter("%(asctime)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger
