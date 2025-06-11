"""
The beacon length should stand out from regular traffic.(2)
This will be? measured by capturing stats from the swarm (1)
The stats captured will also be used to create timing information (3)
    will be used to load test the system as well as test for beacon distortion.
"""

# import logging
from queue import Empty
import psutil
import json
from diyims.requests_utils import execute_request
from datetime import datetime
from time import sleep
from multiprocessing.managers import BaseManager
from diyims.ipfs_utils import get_url_dict
from diyims.general_utils import get_DTS, get_shutdown_target
from diyims.want_item_utils import refresh_want_item_dict
from diyims.path_utils import get_path_dict, get_unique_file
from diyims.config_utils import get_beacon_config_dict, get_satisfy_config_dict
from diyims.logger_utils import get_logger
from diyims.database_utils import (
    set_up_sql_operations,
    refresh_clean_up_dict,
    insert_clean_up_row,
    select_shutdown_entry,
)


def beacon_main():
    # import psutil

    p = psutil.Process()
    p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # TODO: put in config
    logging_enabled = 0  # TODO: config
    beacon_config_dict = get_beacon_config_dict()
    conn, queries = set_up_sql_operations(beacon_config_dict)  # + 1
    Rconn, Rqueries = set_up_sql_operations(beacon_config_dict)  # + 1
    logger = get_logger(
        beacon_config_dict["log_file"],
        "none",
    )

    url_dict = get_url_dict()
    response, status_code, response_dict = execute_request(
        url_key="id",
        logger=logger,
        url_dict=url_dict,
        config_dict=beacon_config_dict,
    )
    self = response_dict["ID"]

    wait_seconds = int(beacon_config_dict["wait_before_startup"])
    logger.debug(f"Waiting for {wait_seconds} seconds before startup.")
    sleep(wait_seconds)  # Config value
    logger.info("Beacon startup.")
    # target_DT = get_shutdown_target(beacon_config_dict)
    # purge_want_items()
    # logger.info("Purge want item files complete")
    target_DT = get_shutdown_target(beacon_config_dict)
    current_DT = datetime.now()

    max_intervals = int(beacon_config_dict["max_intervals"])
    logger.debug(f"Shutdown target {target_DT} or {max_intervals} intervals.")
    beacon_interval = 0
    satisfy_dict = {}
    q_server_port = int(beacon_config_dict["q_server_port"])
    queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
    queue_server.register("get_satisfy_queue")
    queue_server.register("get_beacon_queue")
    queue_server.connect()
    out_bound = queue_server.get_satisfy_queue()
    in_bound = queue_server.get_beacon_queue()
    while target_DT > current_DT and beacon_interval < max_intervals:
        for _ in range(int(beacon_config_dict["number_of_periods"])):
            shutdown_row_dict = select_shutdown_entry(
                Rconn,
                Rqueries,
            )
            if shutdown_row_dict["enabled"]:
                break
            beacon_CID, want_item_file = create_beacon_CID(
                logger,
                beacon_config_dict,
                self,
                logging_enabled,
                conn,
                queries,
                Rconn,
                Rqueries,
            )

            satisfy_dict["status"] = "run"
            satisfy_dict["wait_time"] = beacon_config_dict["short_period_seconds"]
            satisfy_dict["want_item_file"] = want_item_file
            out_bound.put_nowait(satisfy_dict)
            try:
                message = (
                    in_bound.get()
                )  # wait for satisfy to respond or external shutdown
            except Empty:
                pass
            shutdown_row_dict = select_shutdown_entry(
                Rconn,
                Rqueries,
            )
            if shutdown_row_dict["enabled"]:
                break

            if logging_enabled:
                logger.debug(message)

            flash_beacon(
                logger, beacon_config_dict, beacon_CID, logging_enabled
            )  # wait for satisfy to satisfy after a wait period or shutdown
            shutdown_row_dict = select_shutdown_entry(
                Rconn,
                Rqueries,
            )
            if shutdown_row_dict["enabled"]:
                break

        for _ in range(int(beacon_config_dict["number_of_periods"])):
            shutdown_row_dict = select_shutdown_entry(
                Rconn,
                Rqueries,
            )
            if shutdown_row_dict["enabled"]:
                break
            beacon_CID, want_item_file = create_beacon_CID(
                logger,
                beacon_config_dict,
                self,
                logging_enabled,
                conn,
                queries,
                Rconn,
                Rqueries,
            )
            satisfy_dict["status"] = "run"
            satisfy_dict["wait_time"] = beacon_config_dict["long_period_seconds"]
            satisfy_dict["want_item_file"] = want_item_file
            out_bound.put_nowait(satisfy_dict)
            try:
                message = (
                    in_bound.get()
                )  # wait for satisfy response or external shutdown
            except Empty:
                pass
            shutdown_row_dict = select_shutdown_entry(
                Rconn,
                Rqueries,
            )
            if shutdown_row_dict["enabled"]:
                break

            if logging_enabled:
                logger.debug(message)

            flash_beacon(logger, beacon_config_dict, beacon_CID, logging_enabled)
            shutdown_row_dict = select_shutdown_entry(
                Rconn,
                Rqueries,
            )
            if shutdown_row_dict["enabled"]:
                break
        beacon_interval += 1
        current_DT = datetime.now()
        shutdown_row_dict = select_shutdown_entry(
            Rconn,
            Rqueries,
        )
        if shutdown_row_dict["enabled"]:
            break
    Rconn.close()
    conn.close()

    satisfy_dict["status"] = "shutdown"
    out_bound.put_nowait(satisfy_dict)

    logger.info("Beacon shutdown.")
    return


def create_beacon_CID(
    logger, beacon_config_dict, self, logging_enabled, conn, queries, Rconn, Rqueries
):
    url_dict = get_url_dict()
    path_dict = get_path_dict()
    # +1
    # conn, queries = set_up_sql_operations(beacon_config_dict)
    header_row = Rqueries.select_first_peer_row_entry_pointer(
        Rconn, peer_ID=self
    )  # TODO: drop the pointer

    want_item_dict = refresh_want_item_dict()  # TODO: rename to template
    want_item_dict["peer_row_CID"] = header_row["object_CID"]
    want_item_dict["DTS"] = get_DTS()
    # -1
    # conn.close()
    want_item_path = path_dict["want_item_path"]
    proto_item_file = path_dict["want_item_file"]
    want_item_file = get_unique_file(want_item_path, proto_item_file)

    beacon_pin_enabled = int(beacon_config_dict["beacon_pin_enabled"])

    if beacon_pin_enabled:
        param = {"only-hash": "true", "pin": "true", "cid-version": 1}
    else:
        param = {"only-hash": "true", "pin": "false", "cid-version": 1}

    with open(want_item_file, "w") as write_file:
        json.dump(want_item_dict, write_file, indent=4)

    f = open(want_item_file, "rb")
    file = {"file": f}
    response, status_code, response_dict = execute_request(
        url_key="add",
        logger=logger,
        url_dict=url_dict,
        config_dict=beacon_config_dict,
        param=param,
        file=file,
    )
    f.close()

    # json_dict = json.loads(response.text)
    first_peer_table_entry_CID = response_dict["Hash"]
    beacon_CID = first_peer_table_entry_CID
    if logging_enabled:
        logger.debug(f"Create {beacon_CID}")
    # +1
    # conn, queries = set_up_sql_operations(beacon_config_dict)
    clean_up_dict = refresh_clean_up_dict()  # TODO: rename template``
    clean_up_dict["DTS"] = get_DTS()
    clean_up_dict["want_item_file"] = str(want_item_file)
    clean_up_dict["beacon_CID"] = beacon_CID
    insert_clean_up_row(conn, queries, clean_up_dict)
    conn.commit()
    # conn.close()  # -1
    return beacon_CID, want_item_file


def flash_beacon(logger, beacon_config_dict, beacon_CID, logging_enabled):
    url_dict = get_url_dict()

    execute_request(
        url_key="get",
        logger=logger,
        url_dict=url_dict,
        config_dict=beacon_config_dict,
        param={
            "arg": beacon_CID,
        },
    )
    if logging_enabled:
        logger.debug("Flash off")

    return


def satisfy_main():
    """
    Rapid shutdown is initiated from here
    otherwise from beacon.
    """
    # import psutil

    p = psutil.Process()
    p.nice(psutil.ABOVE_NORMAL_PRIORITY_CLASS)
    config_dict = get_satisfy_config_dict()
    logging_enabled = 0

    Rconn, Rqueries = set_up_sql_operations(config_dict)  # + 1
    logger = get_logger(
        config_dict["log_file"],
        "none",
    )

    wait_seconds = int(config_dict["wait_before_startup"])
    logger.debug(f"Waiting for {wait_seconds} seconds before startup.")
    sleep(wait_seconds)  # config value
    logger.info("Satisfy startup.")
    # logger.info("Shutdown signal comes from Beacon.")

    q_server_port = int(config_dict["q_server_port"])
    queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
    queue_server.register("get_satisfy_queue")
    queue_server.register("get_beacon_queue")
    queue_server.connect()
    in_bound = queue_server.get_satisfy_queue()
    out_bound = queue_server.get_beacon_queue()
    satisfy_dict = (
        in_bound.get()
    )  # wait for beacon to provide a wait time ans a beacon_cid
    while satisfy_dict["status"] == "run":
        want_item_file = satisfy_dict["want_item_file"]
        out_bound.put_nowait(satisfy_dict)
        try:
            satisfy_dict = in_bound.get(timeout=int(satisfy_dict["wait_time"]))
        except Empty:
            pass
        satisfy_beacon(logger, config_dict, want_item_file, logging_enabled)
        shutdown_row_dict = select_shutdown_entry(Rconn, Rqueries)
        if shutdown_row_dict["enabled"]:
            break

    Rconn.close()

    out_bound.put_nowait(satisfy_dict)
    logger.info("Satisfy shutdown.")
    return


def satisfy_beacon(logger, config_dict, want_item_file, logging_enabled):
    url_dict = get_url_dict()

    param = {"only-hash": "false", "pin": "true", "cid-version": 1}

    f = open(want_item_file, "rb")
    file = {"file": f}
    execute_request(
        url_key="add",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        param=param,
        file=file,
    )
    f.close()
    if logging_enabled:
        logger.debug(f"Satisfy {want_item_file}")

    return
