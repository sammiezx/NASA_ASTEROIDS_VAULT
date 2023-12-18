import logging

def get_logger(log_file_path):
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    logging.basicConfig(level=logging.INFO)

    return logging.getLogger(__name__ + '.my_procedure')