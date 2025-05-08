from time import sleep

from diyims.beacon import beacon_main, satisfy_main
from diyims.peer_capture import capture_peer_main
from diyims.capture_want_lists import capture_peer_want_lists
from diyims.ipfs_utils import wait_on_ipfs, publish_main
from diyims.logger_utils import get_logger, logger_server_main
from diyims.config_utils import get_scheduler_config_dict
from diyims.queue_server import queue_main
from diyims.database_utils import reset_peer_table_status
from diyims.general_utils import select_local_peer_and_update_metrics

from multiprocessing import Process, set_start_method, freeze_support


def scheduler_main():
    if __name__ != "__main__":
        freeze_support()
        set_start_method("spawn")
    scheduler_config_dict = get_scheduler_config_dict()
    logger = get_logger(scheduler_config_dict["log_file"], "none")
    wait_on_ipfs(logger)
    wait_seconds = int(scheduler_config_dict["wait_before_startup"])
    logger.debug(f"Waiting for {wait_seconds} seconds before startup.")
    sleep(wait_seconds)
    logger.info("Startup of Scheduler.")
    logger.info("Shutdown is dependent upon the shutdown of the scheduled tasks")

    queue_server_main_process = Process(target=queue_main)  # 1
    sleep(int(scheduler_config_dict["submit_delay"]))
    queue_server_main_process.start()
    logger.debug("queue_server_main started.")

    if (
        scheduler_config_dict["publish_enable"] == "True"
        or scheduler_config_dict["publish_enable"] == "Only"
    ):
        publish_main_process = Process(target=publish_main, args=("Normal",))  # 1
        sleep(int(scheduler_config_dict["submit_delay"]))
        publish_main_process.start()
        logger.debug("publish_main started.")

    if scheduler_config_dict["reset_enable"] == "True":
        reset_peer_table_status_process = Process(target=reset_peer_table_status)
        sleep(int(scheduler_config_dict["submit_delay"]))
        reset_peer_table_status_process.start()
        logger.debug("reset peer table status started.")
        reset_peer_table_status_process.join()
        logger.debug("reset peer table status completed.")

    if scheduler_config_dict["metrics_enable"] == "True":
        select_local_peer_and_update_metrics_process = Process(
            target=select_local_peer_and_update_metrics
        )
        sleep(int(scheduler_config_dict["submit_delay"]))
        select_local_peer_and_update_metrics_process.start()
        logger.debug("update metrics started.")
        select_local_peer_and_update_metrics_process.join()
        logger.debug("update metrics completed.")

    if scheduler_config_dict["beacon_enable"] == "True":
        beacon_main_process = Process(  # 2
            target=beacon_main,
        )
        sleep(int(scheduler_config_dict["submit_delay"]))
        beacon_main_process.start()
        logger.debug("beacon_main started.")

        satisfy_main_process = Process(  # 3
            target=satisfy_main,
        )
        sleep(int(scheduler_config_dict["submit_delay"]))
        satisfy_main_process.start()
        logger.debug("satisfy_main started.")

    if scheduler_config_dict["provider_enable"] == "True":
        logger_server_provider_process = Process(  # 4
            target=logger_server_main, args=("PP",)
        )
        sleep(int(scheduler_config_dict["submit_delay"]))
        logger_server_provider_process.start()
        logger.debug("logger_server_provider started.")
        capture_provider_want_lists_process = Process(  # 5
            target=capture_peer_want_lists, args=("PP",)
        )
        sleep(int(scheduler_config_dict["submit_delay"]))
        capture_provider_want_lists_process.start()
        logger.debug("capture_provider_want_lists started.")
        capture_provider_process = Process(target=capture_peer_main, args=("PP",))  # 6
        sleep(int(scheduler_config_dict["submit_delay"]))
        capture_provider_process.start()
        logger.debug("capture_provider_main started.")

    if scheduler_config_dict["bitswap_enable"] == "True":
        logger_server_bitswap_process = Process(target=logger_server_main, args=("BP",))
        sleep(int(scheduler_config_dict["submit_delay"]))
        logger_server_bitswap_process.start()
        logger.debug("logger_server_bitswap started.")
        capture_bitswap_want_lists_process = Process(
            target=capture_peer_want_lists, args=("BP",)
        )
        sleep(int(scheduler_config_dict["submit_delay"]))
        capture_bitswap_want_lists_process.start()
        logger.debug("capture_bitswap_want_lists started.")
        capture_bitswap_process = Process(target=capture_peer_main, args=("BP",))
        sleep(int(scheduler_config_dict["submit_delay"]))
        capture_bitswap_process.start()
        logger.debug("capture_bitswap_main started.")

    if scheduler_config_dict["swarm_enable"] == "True":
        logger_server_swarm_process = Process(target=logger_server_main, args=("SP",))
        sleep(int(scheduler_config_dict["submit_delay"]))
        logger_server_swarm_process.start()
        logger.debug("logger_server_swarm started.")
        capture_swarm_want_lists_process = Process(
            target=capture_peer_want_lists, args=("SP",)
        )
        sleep(int(scheduler_config_dict["submit_delay"]))
        capture_swarm_want_lists_process.start()
        logger.debug("capture_swarm_want_lists started.")
        capture_swarm_process = Process(target=capture_peer_main, args=("SP",))
        sleep(int(scheduler_config_dict["submit_delay"]))
        capture_swarm_process.start()
        logger.debug("capture_swarm_main started.")

    if scheduler_config_dict["publish_enable"] == "Only":
        publish_main_process.join()
    if scheduler_config_dict["beacon_enable"] == "True":
        beacon_main_process.join()
        satisfy_main_process.join()
    if scheduler_config_dict["provider_enable"] == "True":
        capture_provider_process.join()
        capture_provider_want_lists_process.join()
    if scheduler_config_dict["bitswap_enable"] == "True":
        capture_bitswap_process.join()
        capture_bitswap_want_lists_process.join()
    if scheduler_config_dict["swarm_enable"] == "True":
        capture_swarm_process.join()
        capture_swarm_want_lists_process.join()

    if scheduler_config_dict["publish_enable"] == "True":
        logger.info("issuing terminate of publish .")
        publish_main_process.terminate()

    if scheduler_config_dict["provider_enable"] == "True":
        logger.info("issuing terminate of logger_server_provider .")
        logger_server_provider_process.terminate()
    if scheduler_config_dict["bitswap_enable"] == "True":
        logger.info("issuing terminate of logger_server_bitswap .")
        logger_server_bitswap_process.terminate()
    if scheduler_config_dict["swarm_enable"] == "True":
        logger.info("issuing terminate of logger_server_swarm .")
        logger_server_swarm_process.terminate()

    logger.info("issuing terminate of queue_server .")
    queue_server_main_process.terminate()

    logger.info("Normal shutdown of Scheduler.")

    return


if __name__ == "__main__":
    freeze_support()
    set_start_method("spawn")
    scheduler_main()
