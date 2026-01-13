import logging


def setup_script_logging():
    """
    Has two separate handlers: one for console (INFO level) and one 
    for file (DEBUG level).
    """
    # Set up logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG) 

    console_handler = logging.StreamHandler() # for the terminal
    console_handler.setLevel(logging.INFO)

    file_handler = logging.FileHandler('wind_etl.log') # for the log file
    file_handler.setLevel(logging.DEBUG)

    console_format = logging.Formatter('%(levelname)s: %(message)s')
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler.setFormatter(console_format)
    file_handler.setFormatter(file_format)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def setup_orchestration_logging():
    """
    Simple logging setup for orchestration context (e.g., Dagster).
    """
    # Set up logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG) 

    console_handler = logging.StreamHandler() # for the terminal
    console_handler.setLevel(logging.DEBUG)

    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)

    logger.addHandler(console_handler)