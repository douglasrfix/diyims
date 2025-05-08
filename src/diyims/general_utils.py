import sqlite3
from datetime import datetime, timezone, timedelta, date
from dateutil.parser import parse

import aiosql
from pathlib import Path

from diyims.path_utils import get_path_dict
from diyims.py_version_dep import get_sql_str
from diyims.config_utils import get_clean_up_config_dict, get_beacon_config_dict
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
    agent = "0.0.0a60"  # NOTE: How to extract at run time

    return agent


def get_shutdown_target(config_dict):
    current_date = datetime.today()
    shutdown_time = config_dict["shutdown_time"]
    if shutdown_time == "99:99:99":
        shutdown_time = str(current_date + timedelta(weeks=10))
    target_DT = parse(shutdown_time, default=current_date)

    return target_DT


def clean_up():
    clean_up_config_dict = get_clean_up_config_dict()
    beacon_config_dict = get_beacon_config_dict()
    beacon_pin_enabled = int(beacon_config_dict["beacon_pin_enabled"])
    logger = get_logger(
        clean_up_config_dict["log_file"],
        "none",
    )

    url_dict = get_url_dict()
    days_to_delay = clean_up_config_dict["days_to_delay"]
    end_date = date.today() - timedelta(days=int(days_to_delay))

    conn, queries = set_up_sql_operations_list(clean_up_config_dict)
    clean_up_dict = refresh_clean_up_dict()
    clean_up_dict["DTS"] = end_date.isoformat()
    clean_up_tuples, key_dict = select_clean_up_rows_by_date(
        conn, queries, clean_up_dict
    )
    delete_log_rows_by_date(conn, queries, clean_up_dict)
    conn.commit()
    delete_want_list_table_rows_by_date(conn, queries, clean_up_dict)
    conn.commit()

    conn.close()
    conn, queries = set_up_sql_operations(clean_up_config_dict)

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

        if beacon_pin_enabled:
            response, status_code, response_dict = execute_request(
                url_key="pin_remove",
                logger=logger,
                url_dict=url_dict,
                config_dict=clean_up_config_dict,
                param=param,
            )

        delete_clean_up_row_by_date(conn, queries, clean_up_dict)

        conn.commit()
    conn.close()

    return


def select_local_peer_and_update_metrics():
    import json
    from diyims.platform_utils import get_python_version, test_os_platform
    from diyims.ipfs_utils import test_ipfs_version
    from diyims.config_utils import get_want_list_config_dict
    from diyims.database_utils import (
        set_up_sql_operations,
        refresh_peer_row_from_template,
        select_peer_table_local_peer_entry,
        update_peer_table_metrics,
        export_local_peer_row,
    )
    from diyims.general_utils import get_DTS
    from diyims.path_utils import get_path_dict, get_unique_file
    from diyims.logger_utils import (
        get_logger,
    )  # TODO: pass in config_dict perhaps a generic config dictionary
    from diyims.ipfs_utils import get_url_dict
    from diyims.header_utils import ipfs_header_add

    config_dict = get_want_list_config_dict()
    logger = get_logger(
        config_dict["log_file"],
        "none",
    )
    url_dict = get_url_dict()
    path_dict = get_path_dict()

    conn, queries = set_up_sql_operations(config_dict)

    peer_table_dict = refresh_peer_row_from_template()
    peer_table_entry = select_peer_table_local_peer_entry(
        conn, queries, peer_table_dict
    )

    IPFS_agent = test_ipfs_version()
    os_platform = test_os_platform()
    python_version = get_python_version()
    agent = get_agent()

    changed_metrics = False

    if peer_table_entry["execution_platform"] != os_platform:
        peer_table_dict["execution_platform"] = os_platform
        changed_metrics = True
    else:
        peer_table_dict["execution_platform"] = os_platform

    if peer_table_entry["python_version"] != python_version:
        peer_table_dict["python_version"] = python_version
        changed_metrics = True
    else:
        peer_table_dict["python_version"] = python_version

    if peer_table_entry["IPFS_agent"] != IPFS_agent:
        peer_table_dict["IPFS_agent"] = IPFS_agent
        changed_metrics = True
    else:
        peer_table_dict["IPFS_agent"] = IPFS_agent

    if peer_table_entry["agent"] != agent:
        peer_table_dict["agent"] = agent
        changed_metrics = True
    else:
        peer_table_dict["agent"] = agent

    if changed_metrics:
        DTS = get_DTS()
        peer_table_dict["origin_update_DTS"] = DTS

        update_peer_table_metrics(conn, queries, peer_table_dict)
        conn.commit()

        peer_row_dict = export_local_peer_row(config_dict)

        proto_path = path_dict["peer_path"]
        proto_file = path_dict["peer_file"]
        proto_file_path = get_unique_file(proto_path, proto_file)

        param = {"cid-version": 1, "only-hash": "false", "pin": "true"}
        with open(proto_file_path, "w") as write_file:
            json.dump(peer_row_dict, write_file, indent=4)

        f = open(proto_file_path, "rb")
        add_file = {"file": f}
        response, status_code, response_dict = execute_request(
            url_key="add",
            logger=logger,
            url_dict=url_dict,
            config_dict=config_dict,
            file=add_file,
            param=param,
        )
        f.close()

        peer_ID = peer_row_dict["peer_ID"]
        object_CID = response_dict["Hash"]
        object_type = "peer_row_entry"

        ipfs_header_add(
            DTS,
            object_CID,
            object_type,
            peer_ID,
            config_dict,
            logger,
        )

    conn.close()

    return


if __name__ == "__main__":
    select_local_peer_and_update_metrics()
