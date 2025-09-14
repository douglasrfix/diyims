import json
import os
from queue import Empty
from multiprocessing import set_start_method, freeze_support

# from diyims.platform_utils import get_python_version, test_os_platform
# from diyims.ipfs_utils import test_ipfs_version
# from diyims.class_imports import SetSelfReturn
from diyims.config_utils import get_peer_table_maint_config_dict
from diyims.database_utils import (
    set_up_sql_operations,
    # refresh_peer_row_from_template,
    # select_peer_table_local_peer_entry,
    # update_peer_table_metrics,
    # export_local_peer_row,
    update_peer_table_status_to_NPC,
    update_peer_table_status_to_NPC_no_update,
    # select_shutdown_entry,
)
from diyims.general_utils import get_DTS, shutdown_query, set_controls, set_self
from diyims.path_utils import get_path_dict, get_unique_file
from diyims.logger_utils import add_log

# from diyims.ipfs_utils import get_url_dict
from diyims.header_utils import ipfs_header_add

# from diyims.ipfs_utils import export_peer_table
from diyims.requests_utils import execute_request

# from datetime import datetime
from time import sleep
from multiprocessing.managers import BaseManager

# from sqlite3 import IntegrityError
# from diyims.config_utils import get_beacon_config_dict
from diyims.database_utils import (
    #    set_up_sql_operations,
    #    insert_header_row,
    # refresh_log_dict,
    # insert_log_row,
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


def peer_maintenance_main(call_stack: str) -> None:
    """
    docstring
    """

    if __name__ != "__main__":
        freeze_support()
        try:
            set_start_method("spawn")
        except RuntimeError:
            pass

    call_stack = call_stack + ":peer_table_maintenance_main"
    path_dict = get_path_dict()
    config_dict = get_peer_table_maint_config_dict()
    SetControlsReturn = set_controls(call_stack, config_dict)
    SetSelfReturn = set_self()

    wait_before_startup = int(config_dict["wait_before_startup"])
    if SetControlsReturn.debug_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Waiting for {wait_before_startup} seconds before startup.",
        )
    sleep(wait_before_startup)

    add_log(
        process=call_stack,
        peer_type="status",
        msg="Peer Table Maintenance startup.",
    )

    if SetControlsReturn.queues_enabled:
        q_server_port = int(config_dict["q_server_port"])
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
        queue_server.register("get_peer_maint_queue")
        queue_server.connect()
        in_bound = queue_server.get_peer_maint_queue()
    status_code = 200
    while True:
        if shutdown_query(call_stack):
            break
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
            if shutdown_query(call_stack):
                break

            peer_row_CID = peer["version"]
            peer_ID = peer["peer_ID"]
            peer_type = peer["peer_type"]

            # header_CID = header["header_CID"]

            status_code, peer_verified = verify_peer_and_update(
                call_stack,
                peer_row_CID,
                config_dict,
                path_dict,
                peer_ID,
                SetSelfReturn.self,
                peer_type,
                SetControlsReturn.logging_enabled,
                SetControlsReturn.debug_enabled,
                SetControlsReturn.component_test,
                SetControlsReturn.queues_enabled,
            )

            if status_code != 200:
                if SetControlsReturn.logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="Error",
                        msg="Peer Maintenance Panic.",
                    )
                break
        if shutdown_query(call_stack):
            break
        wait_time = int(config_dict["wait_time"])
        try:
            if SetControlsReturn.queues_enabled:
                in_bound.get(timeout=wait_time)
            else:
                sleep(wait_time)

            if shutdown_query(call_stack):
                break

        except Empty:
            if shutdown_query(call_stack):
                break

    add_log(
        process=call_stack,
        peer_type="status",
        msg="Peer Maintenance complete",
    )
    return


def verify_peer_and_update(
    call_stack,
    peer_row_CID,
    config_dict,
    path_dict,
    peer_ID,
    self,
    peer_type,
    logging_enabled,
    debug_enabled,
    component_test,
    queues_enabled,
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
    call_stack = call_stack + ":verify_peer_and_update"
    status_code = 200
    # peer_row_dict = refresh_peer_row_from_template()
    # peer_row_dict["peer_ID"] = peer_ID

    # peer_row_entry = select_peer_table_entry_by_key(Rconn, Rqueries, peer_row_dict)

    # if peer_row_entry["processing_status"] == "WLX":

    status_code, peer_verified, peer_row_verify_dict = verify_peer_row_from_cid(
        call_stack, peer_row_CID, config_dict
    )

    if peer_verified:  # the result of verifying the peer_row_cid cached in the peer row by a beacon_CID or maintenance entry from remote peer monitor.
        peer_row_verify_dict["signature_valid"] = peer_verified
        peer_row_verify_dict["local_update_DTS"] = get_DTS()

        conn, queries = set_up_sql_operations(config_dict)
        peer_table_entry = select_peer_table_entry_by_key(
            conn, queries, peer_row_verify_dict
        )
        conn.close()
        current_origin_value = peer_table_entry[
            "origin_update_DTS"
        ]  # local database values
        new_origin_value = peer_row_verify_dict[
            "origin_update_DTS"
        ]  # potential new values

        if (
            current_origin_value is None
            or peer_table_entry["processing_status"] == "PMP"
        ):  # PMP comes from header chain maint
            conn, queries = set_up_sql_operations(config_dict)
            update_peer_table_status_to_NPC(conn, queries, peer_row_verify_dict)
            conn.commit()

            log_string = f"Peer {peer_row_verify_dict['peer_ID']}  {peer_row_verify_dict['peer_type']} {peer_type} updated."
            if logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=log_string,
                )

        else:
            if current_origin_value < new_origin_value:
                conn, queries = set_up_sql_operations(config_dict)
                update_peer_table_status_to_NPC(conn, queries, peer_row_verify_dict)
                conn.commit()

                log_string = f"Peer {peer_row_verify_dict['peer_ID']}  {peer_row_verify_dict['peer_type']} {peer_type} updated."
            else:  # this would happen if we are processing updates that occurred prior to the current entry created bt a beacon_CID
                conn, queries = set_up_sql_operations(config_dict)
                update_peer_table_status_to_NPC_no_update(
                    conn, queries, peer_row_verify_dict
                )
                conn.commit()

                log_string = f"Peer {peer_row_verify_dict['peer_ID']}  {peer_row_verify_dict['peer_type']} {peer_type} updated to clear NPP."
                if logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=log_string,
                    )

        # if first time for peer put out a header
        if current_origin_value is None:
            proto_path = path_dict["header_path"]
            proto_file = path_dict["header_file"]
            proto_file_path = get_unique_file(proto_path, proto_file)

            param = {"cid-version": 1, "only-hash": "false", "pin": "true"}

            with open(
                proto_file_path, "w", encoding="utf-8", newline="\n"
            ) as write_file:
                json.dump(peer_row_verify_dict, write_file, indent=4)

            f = open(proto_file_path, "rb")
            add_file = {"file": f}
            response, status_code, response_dict = execute_request(
                url_key="add",
                param=param,
                file=add_file,
                call_stack=call_stack,
                http_500_ignore=False,
            )
            f.close()

            if status_code == 200:  # this is a local ipfs problem
                object_CID = response_dict["Hash"]  # new peer_row_cid
            else:
                add_log(
                    process=call_stack,
                    peer_type="Error",
                    msg="Object_CID add failed with {status_code}.",  # TODO: better error handling
                )
                return status_code, peer_verified

            # object_CID = peer_row_CID
            DTS = get_DTS()

            if peer_type == "PP":
                object_type = "provider_peer_entry"  # comes from want list processing one entry per peer row
            else:
                object_type = "remote_peer_entry"  # comes from peer monitor processing one entry per peer row
            if queues_enabled:
                mode = "Normal"
            else:
                mode = "init"
            peer_ID = self
            processing_status = DTS

            status_code, header_CID = ipfs_header_add(
                call_stack,
                DTS,
                object_CID,
                object_type,
                peer_ID,  # =self
                config_dict,
                mode,
                processing_status,
            )

            if status_code != 200:
                if logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="Error",
                        msg="Peer and Update Panic.",
                    )
                return status_code, peer_verified

    else:  # TODO: disable peer from remote monitoring
        log_string = f"Peer {peer_row_CID} signature not valid."  # How should we update the peer record???
        if logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=log_string,
            )

    return status_code, peer_verified


if __name__ == "__main__":
    freeze_support()
    set_start_method("spawn")

    os.environ["DIYIMS_ROAMING"] = "RoamingDev"
    os.environ["COMPONENT_TEST"] = "1"
    os.environ["QUEUES_ENABLED"] = "0"
    os.environ["LOGGING_ENABLED"] = "1"
    peer_maintenance_main("__main__")
