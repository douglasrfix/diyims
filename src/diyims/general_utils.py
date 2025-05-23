import sqlite3
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse

import aiosql
from pathlib import Path

from diyims.path_utils import get_path_dict
from diyims.py_version_dep import get_sql_str
from diyims.config_utils import get_clean_up_config_dict
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


def get_DTS():
    DTS = datetime.now(timezone.utc).isoformat()

    return DTS


def get_agent():
    agent = "0.0.0a78"  # NOTE: How to extract at run time

    return agent


def get_shutdown_target(config_dict):
    current_date = datetime.today()
    shutdown_time = config_dict["shutdown_time"]
    if shutdown_time == "99:99:99":
        shutdown_time = str(current_date + timedelta(weeks=10))
    target_DT = parse(shutdown_time, default=current_date)

    return target_DT


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
    delete_log_rows_by_date(conn, queries, clean_up_dict)
    conn.commit()
    delete_want_list_table_rows_by_date(conn, queries, clean_up_dict)
    conn.commit()

    conn.close()
    conn, queries = set_up_sql_operations(config_dict)

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

        delete_clean_up_row_by_date(conn, queries, clean_up_dict)

        conn.commit()
    conn.close()

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
    test()
