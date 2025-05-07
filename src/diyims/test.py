# from datetime import datetime, timedelta, timezone

# from pathlib import Path
# from rich import print
import json
# import requests

from diyims.config_utils import get_want_list_config_dict

# from diyims.path_utils import get_path_dict
from diyims.database_utils import (
    set_up_sql_operations,
    refresh_peer_row_from_template,
    select_peer_table_local_peer_entry,
    update_peer_table_metrics,
    export_local_peer_row,
)

from diyims.requests_utils import execute_request
from diyims.general_utils import get_DTS
from diyims.platform_utils import get_python_version, test_os_platform
from diyims.ipfs_utils import get_url_dict, test_ipfs_version
from diyims.path_utils import get_path_dict
from diyims.logger_utils import get_logger
from diyims.header_utils import ipfs_header_create


def select_local_peer_and_update_metrics():
    want_list_config_dict = get_want_list_config_dict()
    path_dict = get_path_dict()
    url_dict = get_url_dict()
    logger = get_logger(
        want_list_config_dict["log_file"],
        "none",
    )

    DTS = get_DTS()
    conn, queries = set_up_sql_operations(want_list_config_dict)
    peer_table_dict = refresh_peer_row_from_template()
    peer_table_entry = select_peer_table_local_peer_entry(
        conn, queries, peer_table_dict
    )

    IPFS_agent = test_ipfs_version()
    os_platform = test_os_platform()
    python_version = get_python_version()
    agent = "0.0.0a54"
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
        peer_table_dict["origin_update_DTS"] = DTS
        update_peer_table_metrics(conn, queries, peer_table_dict)
        conn.commit()

        peer_row_dict = export_local_peer_row(want_list_config_dict)

        peer_file = path_dict["peer_file"]

        add_params = {"cid-version": 1, "only-hash": "false", "pin": "true"}
        with open(peer_file, "w") as write_file:
            json.dump(peer_row_dict, write_file, indent=4)

        f = open(peer_file, "rb")
        add_files = {"file": f}
        response, status_code, response_dict = execute_request(
            url_key="add",
            logger=logger,
            url_dict=url_dict,
            config_dict=want_list_config_dict,
            file=add_files,
            param=add_params,
        )
        f.close()

        peer_ID = peer_row_dict["peer_ID"]
        object_CID = response_dict["Hash"]
        object_type = "peer_row_entry"

        header_CID, IPNS_name = ipfs_header_create(
            DTS,
            object_CID,
            object_type,
            peer_ID,
        )

    conn.close()

    return


def test():
    select_local_peer_and_update_metrics()
    return


if __name__ == "__main__":
    test()
