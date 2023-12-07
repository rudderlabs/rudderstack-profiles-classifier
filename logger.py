# logger.py
import logging

# Define the logger with the desired name
logger = logging.getLogger("churn_prediction")

# Configure the logger with the desired log level and handlers
logger.setLevel(logging.WARNING)

#file_handler = logging.FileHandler("classifier.log")
#file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

#logger.addHandler(file_handler)
logger.addHandler(console_handler)
