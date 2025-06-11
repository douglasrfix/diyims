from time import sleep

from diyims.beacon import beacon_main, satisfy_main
from diyims.peer_capture import capture_peer_main
from diyims.capture_want_lists import capture_peer_want_lists
from diyims.ipfs_utils import wait_on_ipfs, publish_main
from diyims.logger_utils import get_logger, logger_server_main
from diyims.config_utils import get_scheduler_config_dict
from diyims.queue_server import queue_main
from diyims.database_utils import reset_peer_table_status
from diyims.peer_utils import select_local_peer_and_update_metrics
from diyims.header_chain_utils import monitor_peer_publishing
from diyims.peer_utils import monitor_peer_table_maint

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
    sleep(wait_seconds)  # config value
    logger.info("Scheduler startup.")
    # logger.info("Shutdown is dependent upon the shutdown of the scheduled tasks")

    queue_server_main_process = Process(target=queue_main)  # 1
    sleep(int(scheduler_config_dict["submit_delay"]))
    queue_server_main_process.start()
    logger.debug("queue_server_main started.")

    if (
        scheduler_config_dict["publish_enable"] == "True"
        or scheduler_config_dict["publish_enable"] == "Only"
    ):
        publish_main_process = Process(
            target=publish_main,  # 2
            args=("Normal",),
        )
        sleep(int(scheduler_config_dict["submit_delay"]))
        publish_main_process.start()
        logger.debug("publish_main started.")

        monitor_peer_publishing_main_process = Process(
            target=monitor_peer_publishing  # 3
        )  # 1
        sleep(int(scheduler_config_dict["submit_delay"]))
        monitor_peer_publishing_main_process.start()
        logger.debug("monitor_peer_publishing_main_process started.")

        monitor_peer_table_maint_process = Process(target=monitor_peer_table_maint)  # 4
        sleep(int(scheduler_config_dict["submit_delay"]))
        monitor_peer_table_maint_process.start()
        logger.debug("Monitor peer table maint started.")

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
        beacon_main_process = Process(
            target=beacon_main,  # 5
        )
        sleep(int(scheduler_config_dict["submit_delay"]))
        beacon_main_process.start()
        logger.debug("Beacon_main started.")

        satisfy_main_process = Process(
            target=satisfy_main,  # 6
        )
        sleep(int(scheduler_config_dict["submit_delay"]))
        satisfy_main_process.start()
        logger.debug("Satisfy_main started.")

    if scheduler_config_dict["provider_enable"] == "True":
        logger_server_provider_process = Process(
            target=logger_server_main,
            args=("PP",),  # 7
        )
        sleep(int(scheduler_config_dict["submit_delay"]))
        logger_server_provider_process.start()
        logger.debug("logger_server_provider started.")
        # capture_provider_want_lists_process = Process(  # 5
        #    target=capture_peer_want_lists, args=("PP",)
        # )
        # sleep(int(scheduler_config_dict["submit_delay"]))
        # capture_provider_want_lists_process.start()
        # logger.info("capture_provider_want_lists started.")
        capture_provider_process = Process(target=capture_peer_main, args=("PP",))  # 8
        sleep(int(scheduler_config_dict["submit_delay"]))
        capture_provider_process.start()
        logger.debug("capture_provider_main started.")

    if scheduler_config_dict["bitswap_enable"] == "True":  # TODO: proper names
        # logger_server_bitswap_process = Process(target=logger_server_main, args=("BP",))
        # sleep(int(scheduler_config_dict["submit_delay"]))
        # logger_server_bitswap_process.start()
        # logger.debug("logger_server_bitswap started.")
        # capture_bitswap_want_lists_process = Process(
        #    target=capture_peer_want_lists, args=("BP",)
        # )
        # sleep(int(scheduler_config_dict["submit_delay"]))
        # capture_bitswap_want_lists_process.start()
        # logger.info("capture_bitswap_want_lists started.")
        # capture_bitswap_process = Process(target=capture_peer_main, args=("BP",))
        # sleep(int(scheduler_config_dict["submit_delay"]))
        # capture_bitswap_process.start()
        # logger.info("capture_bitswap_main started.")

        capture_provider_want_lists_process = Process(
            target=capture_peer_want_lists,
            args=("PP",),  # 9
        )
        sleep(int(scheduler_config_dict["submit_delay"]))
        capture_provider_want_lists_process.start()
        logger.debug("capture_provider_want_lists started.")

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
        monitor_peer_publishing_main_process.join()
        monitor_peer_table_maint_process.join()

    if scheduler_config_dict["beacon_enable"] == "True":
        beacon_main_process.join()
        satisfy_main_process.join()
    if scheduler_config_dict["provider_enable"] == "True":
        capture_provider_process.join()
        # capture_provider_want_lists_process.join()
    if scheduler_config_dict["bitswap_enable"] == "True":
        capture_provider_want_lists_process.join()
        # capture_provider_process.join()
        # capture_bitswap_process.join()
        # capture_bitswap_want_lists_process.join()
    if scheduler_config_dict["swarm_enable"] == "True":
        capture_swarm_process.join()
        capture_swarm_want_lists_process.join()

    if scheduler_config_dict["publish_enable"] == "True":
        # logger.info("issuing terminate of publish.")
        publish_main_process.join()
        monitor_peer_publishing_main_process.join()
        monitor_peer_table_maint_process.join()

    if scheduler_config_dict["provider_enable"] == "True":
        logger.debug("issuing terminate of logger_server_provider .")
        logger_server_provider_process.terminate()
    if scheduler_config_dict["bitswap_enable"] == "True":
        logger.debug("issuing terminate of logger_server_bitswap .")
        # logger_server_bitswap_process.terminate()
    if scheduler_config_dict["swarm_enable"] == "True":
        logger.debug("issuing terminate of logger_server_swarm .")
        logger_server_swarm_process.terminate()

    logger.info("Issuing terminate of Queue Server .")
    queue_server_main_process.terminate()

    logger.info("Scheduler shutdown.")

    return


if __name__ == "__main__":
    freeze_support()
    set_start_method("spawn")
    scheduler_main()
