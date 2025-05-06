import logging
from logging.handlers import RotatingFileHandler
from logging.handlers import QueueHandler
from pathlib import Path
from multiprocessing.managers import BaseManager
# from time import sleep

from diyims.path_utils import get_path_dict
from diyims.config_utils import get_logger_config_dict, get_logger_server_config_dict


def get_logger(file, peer_type):
    # TODO: #14 handle keywords and bring file management as a default in logger
    logger_config_dict = get_logger_config_dict()
    path_dict = get_path_dict()
    file_str = peer_type + file
    logger = logging.getLogger(file_str)
    logger.setLevel(logger_config_dict["default_level"])
    formatter = logging.Formatter("{asctime} - {levelname} - {message}", style="{")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logger_config_dict["console_level"])

    if peer_type != "none":
        file_str = peer_type + file
        file_handler = RotatingFileHandler(
            Path(path_dict["log_path"]).joinpath(file_str),
            mode="a",
            maxBytes=100000,
            backupCount=1,
            encoding="utf-8",
            delay=False,
            errors=None,
        )
    if peer_type == "none":
        file_handler = RotatingFileHandler(
            Path(path_dict["log_path"]).joinpath(file),
            mode="a",
            maxBytes=100000,
            backupCount=1,
            encoding="utf-8",
            delay=False,
            errors=None,
        )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logger_config_dict["file_level"])

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def get_logger_server(file, peer_type):
    logger_config_dict = get_logger_server_config_dict()
    path_dict = get_path_dict()

    logger = logging.getLogger("logger_server")
    logger.setLevel(logger_config_dict["default_level"])
    formatter = logging.Formatter("{asctime} - {levelname} - {message}", style="{")

    if peer_type != "none":
        file_str = peer_type + file
        file_handler = RotatingFileHandler(
            Path(path_dict["log_path"]).joinpath(file_str),
            mode="a",
            maxBytes=100000,
            backupCount=1,
            encoding="utf-8",
            delay=False,
            errors=None,
        )
    if peer_type == "none":
        file_handler = RotatingFileHandler(
            Path(path_dict["log_path"]).joinpath(file),
            mode="a",
            maxBytes=100000,
            backupCount=1,
            encoding="utf-8",
            delay=False,
            errors=None,
        )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logger_config_dict["file_level"])

    # logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return


def get_logger_task(peer_type, peer_ID):
    queue_server = BaseManager(address=("127.0.0.1", 50000), authkey=b"abc")
    if peer_type == "PP":
        queue_server.register("get_provider_server_queue")
        queue_server.connect()
        server_queue = queue_server.get_provider_server_queue()
    elif peer_type == "BP":
        queue_server.register("get_bitswap_server_queue")
        queue_server.connect()
        server_queue = queue_server.get_bitswap_server_queue()
    elif peer_type == "SP":
        queue_server.register("get_swarm_server_queue")
        queue_server.connect()
        server_queue = queue_server.get_swarm_server_queue()

    logger_config_dict = get_logger_server_config_dict()

    task_logger = logging.getLogger(peer_ID)
    task_logger.setLevel(logger_config_dict["default_level"])
    task_logger.propagate = False
    # formatter = logging.Formatter("{asctime} - {levelname} - {message}", style="{")
    queue_handler = QueueHandler(server_queue)
    # queue_handler.set_formatter(formatter)
    # task_logger.handlers_clear()
    task_logger.addHandler(queue_handler)

    return task_logger


def logger_server_main(peer_type):
    want_list_config_dict = get_logger_server_config_dict()
    logger = get_logger(
        want_list_config_dict["log_file"],
        peer_type,
    )
    queue_server = BaseManager(address=("127.0.0.1", 50000), authkey=b"abc")
    if peer_type == "PP":
        queue_server.register("get_provider_server_queue")
        queue_server.connect()
        server_queue = queue_server.get_provider_server_queue()
    elif peer_type == "BP":
        queue_server.register("get_bitswap_server_queue")
        queue_server.connect()
        server_queue = queue_server.get_bitswap_server_queue()
    elif peer_type == "SP":
        queue_server.register("get_swarm_server_queue")
        queue_server.connect()
        server_queue = queue_server.get_swarm_server_queue()

    while True:
        # consume a log message, block until one arrives
        message = server_queue.get()
        # log the message
        logger.handle(message)
        # sleep(1)
