import logging

def setup_logging(name):
    """
    Setup logging configuration for the module.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO, # minimum logging level
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S'
    )

    log = logging.getLogger(name)
    return log