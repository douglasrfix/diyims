# import os
from time import sleep
from multiprocessing import Process, set_start_method, freeze_support
from diyims.beacon import beacon_main
from diyims.provider_capture import provider_capture_main
from diyims.wantlist_capture_submit import wantlist_capture_submit_main
from diyims.ipfs_utils import wait_on_ipfs
from diyims.logger_utils import add_log
from diyims.config_utils import get_scheduler_config_dict
from diyims.queue_server import queue_main
from diyims.database_utils import reset_peer_table_status
from diyims.peer_utils import select_local_peer_and_update_metrics
from diyims.monitor_peer_publishing import monitor_peer_publishing_main
from diyims.peer_maintenance import peer_maintenance_main
from diyims.general_utils import clean_up, reset_shutdown, set_controls
from diyims.publish import publish_main


def scheduler_main(call_stack, roaming):
    if __name__ != "__main__":
        freeze_support()
        set_start_method("spawn")

    call_stack = call_stack + ":scheduler_main"
    config_dict = get_scheduler_config_dict()
    SetControlsReturn = set_controls(call_stack, config_dict)

    wait_on_ipfs(call_stack)
    wait_seconds = int(config_dict["wait_before_startup"])
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Waiting for {wait_seconds} seconds before startup.",
        )
    sleep(wait_seconds)  # config value
    add_log(
        process=call_stack,
        peer_type="status",
        msg="Scheduler startup.",
    )

    queue_server_main_process = Process(target=queue_main, args=(call_stack,))
    sleep(int(config_dict["submit_delay"]))
    queue_server_main_process.start()
    add_log(
        process=call_stack,
        peer_type="status",
        msg="Queue Server startup.",
    )

    if config_dict["reset_enable"] == "True":
        reset_peer_table_status_process = Process(
            target=reset_peer_table_status, args=(call_stack,)
        )
        sleep(int(config_dict["submit_delay"]))
        reset_peer_table_status_process.start()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Peer table reset startup.",
        )
        reset_peer_table_status_process.join()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Peer table reset complete.",
        )

        clean_up_process = Process(
            target=clean_up,
            args=(
                call_stack,
                roaming,
            ),
        )
        sleep(int(config_dict["submit_delay"]))
        clean_up_process.start()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Clean up startup.",
        )
        clean_up_process.join()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Clean up complete.",
        )

    if config_dict["metrics_enable"] == "True":
        select_local_peer_and_update_metrics_process = Process(
            target=select_local_peer_and_update_metrics,
            args=(call_stack,),
        )
        sleep(int(config_dict["submit_delay"]))
        select_local_peer_and_update_metrics_process.start()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Metrics update startup.",
        )
        select_local_peer_and_update_metrics_process.join()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Metrics update complete.",
        )

    if config_dict["beacon_enable"] == "True":
        beacon_main_process = Process(
            target=beacon_main,
            args=(call_stack,),
        )
        sleep(int(config_dict["submit_delay"]))
        beacon_main_process.start()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Beacon startup.",
        )

    if config_dict["provider_enable"] == "True":
        capture_provider_process = Process(
            target=provider_capture_main,
            args=(
                call_stack,
                "PP",
            ),
        )
        sleep(int(config_dict["submit_delay"]))
        capture_provider_process.start()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Provider Capture startup.",
        )

    if config_dict["wantlist_enable"] == "True":
        capture_provider_want_lists_process = Process(
            target=wantlist_capture_submit_main,
            args=(
                call_stack,
                "PP",
            ),
        )
        sleep(int(config_dict["submit_delay"]))
        capture_provider_want_lists_process.start()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Want List Capture startup.",
        )

    if config_dict["peer_maint_enable"] == "True":
        peer_table_maintenance_process = Process(
            target=peer_maintenance_main,
            args=(call_stack,),
        )
        sleep(int(config_dict["submit_delay"]))
        peer_table_maintenance_process.start()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Peer Maintenance startup.",
        )

    if config_dict["publish_enable"] == "True":
        publish_main_process = Process(
            target=publish_main,
            args=(
                call_stack,
                "Normal",
            ),
        )
        sleep(int(config_dict["submit_delay"]))
        publish_main_process.start()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Publish startup.",
        )

    if config_dict["remote_monitor_enable"] == "True":
        monitor_peer_publishing_main_process = Process(
            target=monitor_peer_publishing_main,
            args=(call_stack,),
        )
        sleep(int(config_dict["submit_delay"]))
        monitor_peer_publishing_main_process.start()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Remote Monitor startup.",
        )

    if config_dict["beacon_enable"] == "True":
        beacon_main_process.join()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Beacon completed.",
        )
    if config_dict["provider_enable"] == "True":
        capture_provider_process.join()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Provider Capture completed.",
        )
    if config_dict["wantlist_enable"] == "True":
        capture_provider_want_lists_process.join()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Want List Capture completed.",
        )
    if config_dict["peer_maint_enable"] == "True":
        peer_table_maintenance_process.join()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Peer Maintenance completed.",
        )
    if config_dict["publish_enable"] == "True":
        publish_main_process.join()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Publish completed.",
        )
    if config_dict["remote_monitor_enable"] == "True":
        monitor_peer_publishing_main_process.join()
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Remote Monitor completed.",
        )
    queue_server_main_process.terminate()

    # logger.info("Scheduler shutdown.")
    add_log(
        process=call_stack,
        peer_type="status",
        msg="Queue Server terminated.",
    )

    reset_shutdown(call_stack)

    add_log(
        process=call_stack,
        peer_type="status",
        msg="Scheduler completed.",
    )

    return


if __name__ == "__main__":
    freeze_support()
    set_start_method("spawn")
    # monkeypatch.setenv("DIYIMS_ROAMING", "RoamingDev")
    scheduler_main(
        "cmd",
        "RoamingDev",
    )
