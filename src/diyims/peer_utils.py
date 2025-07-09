import json
from queue import Empty
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
    update_peer_table_status_to_NPC_no_update,
    select_shutdown_entry,
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
# from time import sleep
import psutil
from multiprocessing.managers import BaseManager

# from sqlite3 import IntegrityError
# from diyims.config_utils import get_beacon_config_dict
from diyims.database_utils import (
    #    set_up_sql_operations,
    #    insert_header_row,
    refresh_log_dict,
    insert_log_row,
    select_peer_table_entry_by_key,
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
    path_dict = get_path_dict()
    conn, queries = set_up_sql_operations(config_dict)
    # logger = get_logger(
    #    config_dict["log_file"],
    #    "none",
    # )

    logger.info("Peer Maintenance startup.")
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
    in_bound = queue_server.get_peer_maint_queue()

    while True:
        conn, queries = set_up_sql_operations(config_dict)
        # Rconn, Rqueries = set_up_sql_operations(config_dict)
        peer_table_rows = queries.select_peer_table_processing_status_NPP(
            conn
        )  # create list
        peer_list = []
        for row in peer_table_rows:
            peer_list.append(row)
        conn.close()

        for peer in peer_list:  # peer level
            conn, queries = set_up_sql_operations(config_dict)
            shutdown_row_dict = select_shutdown_entry(
                conn,
                queries,
            )
            conn.close()
            if shutdown_row_dict["enabled"]:
                break
            peer_row_CID = peer["version"]
            peer_ID = peer["peer_ID"]
            peer_type = peer["peer_type"]

            # header_CID = header["header_CID"]

            verify_peer_and_update(
                peer_row_CID,
                logger,
                config_dict,
                # conn,
                # queries,
                # Rconn,
                # Rqueries,
                pid,
                url_dict,
                path_dict,
                peer_ID,
                self,
                peer_type,
            )

        try:
            in_bound.get(timeout=600)  # config value
            conn, queries = set_up_sql_operations(config_dict)
            shutdown_row_dict = select_shutdown_entry(
                conn,
                queries,
            )
            conn.close()
            if shutdown_row_dict["enabled"]:
                break
        except Empty:
            conn, queries = set_up_sql_operations(config_dict)
            shutdown_row_dict = select_shutdown_entry(
                conn,
                queries,
            )
            conn.close()
            if shutdown_row_dict["enabled"]:
                break

    logger.info("Peer Maintenance shutdown.")
    return


def verify_peer_and_update(
    peer_row_CID,
    logger,
    config_dict,
    # conn,
    # queries,
    # Rconn,
    # Rqueries,
    pid,
    url_dict,
    path_dict,
    peer_ID,
    self,
    peer_type,
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
        peer_row_dict["signature_valid"] = peer_verified
        peer_row_dict["version"] = 0
        peer_row_dict["local_update_DTS"] = get_DTS()

        conn, queries = set_up_sql_operations(config_dict)
        peer_table_entry = select_peer_table_entry_by_key(conn, queries, peer_row_dict)
        conn.close()
        old_origin_value = peer_table_entry["origin_update_DTS"]
        new_origin_value = peer_row_dict["origin_update_DTS"]

        if (old_origin_value < new_origin_value) or (
            old_origin_value == "null" or peer_table_entry["processing_status"] == "PMP"
        ):
            # TODO: ping peer monitor
            if (old_origin_value < new_origin_value) or old_origin_value == "null":
                conn, queries = set_up_sql_operations(config_dict)
                update_peer_table_status_to_NPC(conn, queries, peer_row_dict)
                conn.commit()

                log_string = f"Peer {peer_row_dict['peer_ID']}  {peer_row_dict['peer_type']} {peer_type} updated."
            else:
                conn, queries = set_up_sql_operations(config_dict)
                update_peer_table_status_to_NPC_no_update(conn, queries, peer_row_dict)
                conn.commit()

                log_string = f"Peer {peer_row_dict['peer_ID']}  {peer_row_dict['peer_type']} {peer_type} updated to clear NPP."

            log_dict = refresh_log_dict()
            log_dict["DTS"] = get_DTS()
            log_dict["process"] = "verify-and-update"
            log_dict["pid"] = pid
            log_dict["peer_type"] = "UP"
            log_dict["msg"] = log_string
            conn, queries = set_up_sql_operations(config_dict)
            insert_log_row(conn, queries, log_dict)
            conn.commit()

        # if first time for peer put out a header
        if old_origin_value == "null":
            proto_path = path_dict["header_path"]
            proto_file = path_dict["header_file"]
            proto_file_path = get_unique_file(proto_path, proto_file)

            param = {"cid-version": 1, "only-hash": "false", "pin": "true"}

            with open(
                proto_file_path, "w", encoding="utf-8", newline="\n"
            ) as write_file:
                json.dump(peer_row_dict, write_file, indent=4)

            f = open(proto_file_path, "rb")
            add_file = {"file": f}
            response, status_code, response_dict = execute_request(
                url_key="add",
                logger=logger,
                url_dict=url_dict,
                config_dict=config_dict,
                param=param,
                file=add_file,
            )
            f.close()

            object_CID = response_dict["Hash"]  # new peer_row_cid

            object_CID = peer_row_CID
            DTS = get_DTS()

            if peer_type == "PP":
                object_type = "provider_peer_entry"  # comes from want list processing one entry per peer row
            else:
                object_type = "remote_peer_entry"  # comes from peer monitor processing one entry per peer row

            mode = "Normal"
            peer_ID = self
            processing_status = DTS

            ipfs_header_add(
                DTS,
                object_CID,
                object_type,
                peer_ID,
                config_dict,
                logger,
                mode,
                # conn,
                # queries,
                processing_status,
                # Rconn,
                # Rqueries,
            )

    else:
        log_string = f"Peer {peer_row_CID} signature not valid."  # How should we update the peer record???

        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "verify_peer-F1"
        log_dict["pid"] = pid
        log_dict["peer_type"] = "VP"
        log_dict["msg"] = log_string
        conn, queries = set_up_sql_operations(config_dict)
        insert_log_row(conn, queries, log_dict)
        conn.commit()

    return peer_verified


def select_local_peer_and_update_metrics():
    """ """
    # TODO: add message
    config_dict = get_metrics_config_dict()
    url_dict = get_url_dict()
    path_dict = get_path_dict()
    conn, queries = set_up_sql_operations(config_dict)
    Rconn, Rqueries = set_up_sql_operations(config_dict)
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
        Rconn, Rqueries, peer_table_dict
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
            logger=logger,
            url_dict=url_dict,
            config_dict=config_dict,
            file=add_file,
            param=param,
        )
        f.close()

        peer_ID = peer_row_dict["peer_ID"]
        object_CID = response_dict["Hash"]  # new peer row cid
        object_type = "local_peer_entry"
        mode = "Normal"
        processing_status = DTS

        ipfs_header_add(
            DTS,
            object_CID,
            object_type,
            peer_ID,
            config_dict,
            logger,
            mode,
            # conn,
            # queries,
            processing_status,
            # Rconn,
            # Rqueries,
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
            processing_status
        )
        """
    # conn.close()

    return


if __name__ == "__main__":
    monitor_peer_table_maint()
