import psutil
from multiprocessing.managers import BaseManager
from queue import Queue
from time import sleep

from diyims.logger_utils import get_logger
from diyims.config_utils import get_queue_config_dict


def queue_main():
    p = psutil.Process()
    p.nice(psutil.ABOVE_NORMAL_PRIORITY_CLASS)  # TODO: put in config
    queue_config_dict = get_queue_config_dict()
    logger = get_logger(queue_config_dict["log_file"], "none")
    wait_seconds = int(queue_config_dict["wait_before_startup"])
    logger.debug(f"Waiting for {wait_seconds} seconds before startup.")
    sleep(wait_seconds)
    logger.info("Startup of Queue Server.")
    logger.info(
        "Shutdown is dependent upon the Scheduler issuing a terminate() against this process"
    )
    q_server_port = int(queue_config_dict["q_server_port"])
    manager = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
    provider_queue = Queue()
    bitswap_queue = Queue()
    swarm_queue = Queue()
    provider_server_queue = Queue()
    bitswap_server_queue = Queue()
    swarm_server_queue = Queue()
    beacon_queue = Queue()
    satisfy_queue = Queue()
    manager.register("get_beacon_queue", callable=lambda: beacon_queue)
    manager.register("get_satisfy_queue", callable=lambda: satisfy_queue)
    manager.register("get_provider_queue", callable=lambda: provider_queue)
    manager.register("get_bitswap_queue", callable=lambda: bitswap_queue)
    manager.register("get_swarm_queue", callable=lambda: swarm_queue)
    manager.register(
        "get_provider_server_queue", callable=lambda: provider_server_queue
    )
    manager.register("get_bitswap_server_queue", callable=lambda: bitswap_server_queue)
    manager.register("get_swarm_server_queue", callable=lambda: swarm_server_queue)
    server = manager.get_server()
    server.serve_forever()

    return


if __name__ == "__main__":
    queue_main()
