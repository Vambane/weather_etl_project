import logging 
import os 
import datetime import datetime

def get_logger(name: str ="weather_etl", log_dir: str="logs"): 

    # Create logs folder if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    # Define logs folder path
    log_file = os.path.join(log_dir, "f{datetime.utcnow().date()}.log")

    #logging configuration

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(name)

