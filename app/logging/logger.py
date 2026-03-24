# logger.py
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logger(
    # name: str,
    log_dir: Path,
    level: int = logging.DEBUG,
    filename: str = "run.log",
    max_bytes: int = 20 * 1024**2,
    backup_count: int = 5,
) -> logging.Logger:
    
    logger = logging.getLogger("weather_agent")
    
    if logger.handlers:
        return logger
    
    logger.setLevel(level)
    logger.propagate = False
    
    log_dir.mkdir(parents=True, exist_ok=True)
    
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    #№ File handler
    file_handler = RotatingFileHandler(
        log_dir / filename,
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    file_handler.setFormatter(fmt)
    
    #№ Console handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    return logger