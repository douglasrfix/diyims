import json
from diyims.platform_utils import get_python_version, test_os_platform
from diyims.ipfs_utils import test_ipfs_version
from diyims.config_utils import get_metrics_config_dict
from diyims.database_utils import (
    set_up_sql_operations,
    refresh_peer_row_from_template,
    select_peer_table_local_peer_entry,
    update_peer_table_metrics,
    export_local_peer_row,
    update_peer_table_status_to_NPC,
)
from diyims.general_utils import get_DTS, get_agent
from diyims.path_utils import get_path_dict, get_unique_file
from diyims.logger_utils import (
    get_logger,
)
from diyims.ipfs_utils import get_url_dict
from diyims.header_utils import ipfs_header_add

# from diyims.ipfs_utils import export_peer_table
from diyims.requests_utils import execute_request

# from datetime import datetime
from time import sleep
import psutil
from multiprocessing.managers import BaseManager

# from sqlite3 import IntegrityError
# from diyims.config_utils import get_beacon_config_dict
from diyims.database_utils import (
    #    set_up_sql_operations,
    #    insert_header_row,
    refresh_log_dict,
    insert_log_row,
)

# from diyims.header_chain_utils import header_chain_maint
from diyims.security_utils import verify_peer_row_from_cid
# from diyims.ipfs_utils import export_peer_table
# from diyims.header_utils import ipfs_header_add
# from diyims.database_utils import refresh_peer_row_from_template
# from diyims.security_utils import verify_peer_row_from_cid
# from diyims.ipfs_utils import export_peer_table
# from diyims.header_utils import ipfs_header_add
# from diyims.database_utils import refresh_peer_row_from_template


def monitor_peer_table_maint():
    """
    docstring
    """

    p = psutil.Process()
    pid = p.pid
    config_dict = get_metrics_config_dict()
    logger = get_logger(
        config_dict["log_file"],
        "none",
    )
    url_dict = get_url_dict()
    url_dict = get_url_dict()
    path_dict = get_path_dict()
    conn, queries = set_up_sql_operations(config_dict)
    logger = get_logger(
        config_dict["log_file"],
        "none",
    )
    response, status_code, response_dict = execute_request(
        url_key="id",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
    )

    self = response_dict["ID"]

    q_server_port = int(config_dict["q_server_port"])
    queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
    queue_server.register("get_peer_maint_queue")
    queue_server.connect()
    # peer_maint_queue = queue_server.get_peer_maint_queue()

    while True:
        conn, queries = set_up_sql_operations(config_dict)
        Rconn, Rqueries = set_up_sql_operations(config_dict)
        peer_table_rows = Rqueries.select_peer_table_processing_status_NPP(Rconn)

        for row in peer_table_rows:  # peer level
            if row["processing_status"] == "NPP":
                peer_row_CID = row["agent"]
                peer_ID = row["peer_ID"]

                verify_peer_and_update(
                    peer_row_CID,
                    logger,
                    config_dict,
                    conn,
                    queries,
                    Rconn,
                    Rqueries,
                    pid,
                    url_dict,
                    path_dict,
                    peer_ID,
                    self,
                )

        conn.close()
        sleep(600)
    return


def verify_peer_and_update(
    peer_row_CID,
    logger,
    config_dict,
    conn,
    queries,
    Rconn,
    Rqueries,
    pid,
    url_dict,
    path_dict,
    peer_ID,
    self,
):
    """_summary_

    Args:
        peer_row_CID (_type_): _description_
        logger (_type_): _description_
        config_dict (_type_): _description_
        conn (_type_): _description_
        queries (_type_): _description_
        Rconn (_type_): _description_
        Rqueries (_type_): _description_
        pid (_type_): _description_
        url_dict (_type_): _description_
        path_dict (_type_): _description_
        peer_ID (_type_): _description_

    Returns:
        _type_: _description_
    """

    # peer_row_dict = refresh_peer_row_from_template()
    # peer_row_dict["peer_ID"] = peer_ID

    # peer_row_entry = select_peer_table_entry_by_key(Rconn, Rqueries, peer_row_dict)

    # if peer_row_entry["processing_status"] == "WLX":

    peer_verified, peer_row_dict = verify_peer_row_from_cid(
        peer_row_CID, logger, config_dict
    )

    if peer_verified:
        peer_row_dict["local_update_DTS"] = get_DTS()

        update_peer_table_status_to_NPC(conn, queries, peer_row_dict)
        conn.commit()

        log_string = f"Peer {peer_row_dict['peer_ID']}  updated."
        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "verify-and-update"
        log_dict["pid"] = pid
        log_dict["peer_type"] = "VP"
        log_dict["msg"] = log_string
        insert_log_row(conn, queries, log_dict)
        conn.commit()

        object_CID = peer_row_CID
        DTS = get_DTS()
        object_type = "validated_remote_peer_entry"
        mode = "Normal"
        peer_ID = self

        ipfs_header_add(
            DTS,
            object_CID,
            object_type,
            peer_ID,
            config_dict,
            logger,
            mode,
            conn,
            queries,
        )

    else:
        log_string = f"Peer {peer_row_dict['peer_entry_CID']} signature not valid."

        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "verify_peer-F1"
        log_dict["pid"] = pid
        log_dict["peer_type"] = "VP"
        log_dict["msg"] = log_string
        insert_log_row(conn, queries, log_dict)
        conn.commit()

    return peer_verified


def select_local_peer_and_update_metrics():
    """ """

    config_dict = get_metrics_config_dict()
    url_dict = get_url_dict()
    path_dict = get_path_dict()
    conn, queries = set_up_sql_operations(config_dict)
    logger = get_logger(
        config_dict["log_file"],
        "none",
    )
    IPFS_agent = test_ipfs_version()
    os_platform = test_os_platform()
    python_version = get_python_version()
    agent = get_agent()

    peer_table_dict = refresh_peer_row_from_template()
    peer_table_entry = select_peer_table_local_peer_entry(
        conn, queries, peer_table_dict
    )

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
        logger.info("Metrics changed processed.")
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

        peer_ID = peer_row_dict["peer_ID"]  # new entry to pint at updated peer row
        object_CID = response_dict["Hash"]
        object_type = "local_peer_row_entry"
        mode = "Normal"

        ipfs_header_add(
            DTS,
            object_CID,
            object_type,
            peer_ID,
            config_dict,
            logger,
            mode,
            conn,
            queries,
        )

        # logger.info("Metrics change processed.")

        """
        object_CID = export_peer_table(
            conn,
            queries,
            url_dict,
            path_dict,
            config_dict,
            logger,
        )

        DTS = get_DTS()
        object_type = "peer_table_entry"
        mode = "Normal"

        ipfs_header_add(  # entry pointing to a new peer table rather than a updated row
            DTS,
            object_CID,
            object_type,
            peer_ID,
            config_dict,
            logger,
            mode,
            conn,
            queries,
        )
        """
    conn.close()

    return


if __name__ == "__main__":
    monitor_peer_table_maint()
