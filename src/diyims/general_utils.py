import sqlite3
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse

import aiosql
from pathlib import Path

from diyims.path_utils import get_path_dict
from diyims.py_version_dep import get_sql_str
from diyims.config_utils import get_clean_up_config_dict, get_shutdown_config_dict
from diyims.logger_utils import get_logger
from diyims.ipfs_utils import get_url_dict
from diyims.database_utils import (
    delete_want_list_table_rows_by_date,
    set_up_sql_operations,
    refresh_clean_up_dict,
    select_clean_up_rows_by_date,
    delete_clean_up_row_by_date,
    set_up_sql_operations_list,
    delete_log_rows_by_date,
    update_shutdown_enabled_1,
)
from diyims.requests_utils import execute_request


def get_network_name():
    path_dict = get_path_dict()
    sql_str = get_sql_str()
    queries = aiosql.from_str(sql_str, "sqlite3")
    connect_path = path_dict["db_file"]
    conn = sqlite3.connect(connect_path)
    conn.row_factory = sqlite3.Row
    query_row = queries.select_network_name(conn)
    network_name = query_row["network_name"]
    conn.close()
    return network_name


def get_DTS() -> str:
    """Generates an iso6??? formatted UTC time string
    suitable for timestamp use. if the value is to be passed to a url query string the
    + in the iso format must be replaced with a %2B

    Returns:
        str: UTC datetime.now() in an iso format
    """
    DTS = datetime.now(timezone.utc).isoformat()

    return DTS


def get_agent():
    agent = "0.0.0a131"  # NOTE: How to extract at run time

    return agent


def get_shutdown_target(config_dict):
    current_date = datetime.today()
    shutdown_time = config_dict["shutdown_time"]
    if shutdown_time == "99:99:99":
        shutdown_time = str(current_date + timedelta(weeks=10))
    target_DT = parse(shutdown_time, default=current_date)

    return target_DT


def shutdown_cmd():
    from multiprocessing.managers import BaseManager

    config_dict = get_shutdown_config_dict()
    conn, queries = set_up_sql_operations(config_dict)
    update_shutdown_enabled_1(
        conn,
        queries,
    )
    conn.commit()
    conn.close()

    q_server_port = int(config_dict["q_server_port"])

    queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
    queue_server.register("get_satisfy_queue")
    queue_server.register("get_beacon_queue")
    queue_server.register("get_provider_queue")
    queue_server.register("get_want_list_queue")
    queue_server.register("get_peer_monitor_queue")
    queue_server.register("get_publish_queue")
    queue_server.register("get_peer_maint_queue")

    try:
        queue_server.connect()
    except ConnectionRefusedError:
        return
    satisfy_queue = queue_server.get_satisfy_queue()
    beacon_queue = queue_server.get_beacon_queue()
    provider_queue = queue_server.get_provider_queue()
    want_list_queue = queue_server.get_want_list_queue()
    peer_monitor_queue = queue_server.get_peer_monitor_queue()
    publish_queue = queue_server.get_publish_queue()
    peer_maint_queue = queue_server.get_peer_maint_queue()
    # order these by the most likely to be in a long wait
    satisfy_queue.put_nowait("shutdown")
    beacon_queue.put_nowait("shutdown")
    provider_queue.put_nowait("shutdown")
    want_list_queue.put_nowait("shutdown")
    peer_monitor_queue.put_nowait("shutdown")
    publish_queue.put_nowait("shutdown")
    peer_maint_queue.put_nowait("shutdown")

    return


def clean_up():
    config_dict = get_clean_up_config_dict()

    logger = get_logger(
        config_dict["log_file"],
        "none",
    )

    url_dict = get_url_dict()
    hours_to_delay = config_dict["hours_to_delay"]
    end_time = datetime.today() - timedelta(hours=int(hours_to_delay))

    conn, queries = set_up_sql_operations_list(config_dict)
    clean_up_dict = refresh_clean_up_dict()
    clean_up_dict["DTS"] = end_time.isoformat()
    clean_up_tuples, key_dict = select_clean_up_rows_by_date(
        conn, queries, clean_up_dict
    )
    conn.close()

    conn, queries = set_up_sql_operations_list(config_dict)
    delete_log_rows_by_date(conn, queries, clean_up_dict)
    conn.commit()
    conn.close()

    conn, queries = set_up_sql_operations_list(config_dict)
    delete_want_list_table_rows_by_date(conn, queries, clean_up_dict)
    conn.commit()
    conn.close()

    for inner_tuple in clean_up_tuples:
        DTS = inner_tuple[key_dict["DTS"]]
        want_item_file = inner_tuple[key_dict["want_item_file"]]
        beacon_CID = inner_tuple[key_dict["beacon_CID"]]
        clean_up_dict = refresh_clean_up_dict()
        clean_up_dict["DTS"] = DTS

        Path(want_item_file).unlink()

        param = {
            "arg": beacon_CID,
        }

        response, status_code, response_dict = execute_request(
            url_key="pin_remove",
            logger=logger,
            url_dict=url_dict,
            config_dict=config_dict,
            param=param,
        )
        conn, queries = set_up_sql_operations(config_dict)
        delete_clean_up_row_by_date(conn, queries, clean_up_dict)

        conn.commit()
        conn.close()

    network_name = get_network_name()

    param = {
        "arg": network_name,
    }

    """
    response, status_code, response_dict = execute_request(
        url_key="pin_add",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        param=param,
    )
    """
    response, status_code, response_dict = execute_request(
        url_key="provide",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        param=param,
    )
    print(status_code)

    return


def test():
    from datetime import datetime
    from time import sleep

    start_DTS = get_DTS()
    start = datetime.fromisoformat(start_DTS)
    sleep(58)
    stop_DTS = get_DTS()
    stop = datetime.fromisoformat(stop_DTS)
    duration = stop - start
    print(duration)


if __name__ == "__main__":
    clean_up()
