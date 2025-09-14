import json
import os

# from queue import Empty
from multiprocessing import set_start_method, freeze_support
from diyims.platform_utils import get_python_version, test_os_platform
from diyims.ipfs_utils import test_ipfs_version
from diyims.config_utils import get_metrics_config_dict
from diyims.database_utils import (
    set_up_sql_operations,
    refresh_peer_row_from_template,
    select_peer_table_local_peer_entry,
    update_peer_table_metrics,
    export_local_peer_row,
    # update_peer_table_status_to_NPC,
    # update_peer_table_status_to_NPC_no_update,
    # select_shutdown_entry,
)
from diyims.general_utils import get_DTS, get_agent
from diyims.path_utils import get_path_dict, get_unique_file
from diyims.logger_utils import add_log
from diyims.ipfs_utils import get_url_dict
from diyims.header_utils import ipfs_header_add

# from diyims.ipfs_utils import export_peer_table
from diyims.requests_utils import execute_request

# from datetime import datetime
# from time import sleep
# from multiprocessing.managers import BaseManager

# from sqlite3 import IntegrityError
# from diyims.config_utils import get_beacon_config_dict
# from diyims.database_utils import (
#    set_up_sql_operations,
#    insert_header_row,
# refresh_log_dict,
# insert_log_row,
# select_peer_table_entry_by_key,
# )

# from diyims.header_chain_utils import header_chain_maint
# m diyims.security_utils import verify_peer_row_from_cid
# from diyims.ipfs_utils import export_peer_table
# from diyims.header_utils import ipfs_header_add
# from diyims.database_utils import refresh_peer_row_from_template
# from diyims.security_utils import verify_peer_row_from_cid
# from diyims.ipfs_utils import export_peer_table
# from diyims.header_utils import ipfs_header_add
# from diyims.database_utils import refresh_peer_row_from_template


def select_local_peer_and_update_metrics(call_stack):
    """ """
    # TODO: add message
    call_stack = call_stack + ":select_local_peer_and_update_metrics"
    config_dict = get_metrics_config_dict()
    url_dict = get_url_dict()
    path_dict = get_path_dict()
    conn, queries = set_up_sql_operations(config_dict)
    Rconn, Rqueries = set_up_sql_operations(config_dict)
    # logger = get_logger(
    #    config_dict["log_file"],
    #    "none",
    # )
    IPFS_agent = test_ipfs_version()
    os_platform = test_os_platform()
    python_version = get_python_version()
    agent = get_agent()

    peer_table_dict = refresh_peer_row_from_template()
    peer_table_entry = select_peer_table_local_peer_entry(
        Rconn, Rqueries, peer_table_dict
    )
    status_code = 200
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
        # logger.info("Metrics changed processed.")
        DTS = get_DTS()
        peer_table_dict["origin_update_DTS"] = DTS

        conn, queries = set_up_sql_operations(config_dict)
        update_peer_table_metrics(conn, queries, peer_table_dict)
        conn.commit()
        conn.close()

        peer_row_dict = export_local_peer_row(config_dict)

        proto_path = path_dict["peer_path"]
        proto_file = path_dict["peer_file"]
        proto_file_path = get_unique_file(proto_path, proto_file)

        param = {"cid-version": 1, "only-hash": "false", "pin": "true"}
        with open(proto_file_path, "w", encoding="utf-8", newline="\n") as write_file:
            json.dump(peer_row_dict, write_file, indent=4)

        f = open(proto_file_path, "rb")
        add_file = {"file": f}
        response, status_code, response_dict = execute_request(
            url_key="add",
            url_dict=url_dict,
            config_dict=config_dict,
            file=add_file,
            param=param,
            call_stack=call_stack,
            http_500_ignore=False,
        )
        f.close()

        if status_code != 200:
            add_log(
                process=call_stack,
                peer_type="Error",
                msg="Update Metrics Object_CID add failed Panic.",
            )
            return status_code

        peer_ID = peer_row_dict["peer_ID"]
        object_CID = response_dict["Hash"]  # new peer row cid
        object_type = "local_peer_entry"
        mode = "Normal"
        processing_status = DTS

        status_code = ipfs_header_add(
            call_stack,
            DTS,
            object_CID,
            object_type,
            peer_ID,
            config_dict,
            mode,
            processing_status,
        )

        if status_code != 200:  # TODO: remove recent pin or defer pin to here
            add_log(
                process=call_stack,
                peer_type="Error",
                msg="Update Metrics Panic.",
            )
            return status_code

    return status_code


if __name__ == "__main__":
    freeze_support()
    set_start_method("spawn")

    os.environ["DIYIMS_ROAMING"] = "RoamingDev"
    os.environ["COMPONENT_TEST"] = "1"
    os.environ["QUEUES_ENABLED"] = "0"
    # peer_table_maintenance_main("__main__")
