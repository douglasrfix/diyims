import json
import os
from queue import Empty
from multiprocessing import set_start_method, freeze_support
from diyims.config_utils import get_peer_table_maint_config_dict

# from diyims.database_utils import (
# set_up_sql_operations,
# update_peer_table_status_to_NPC,
# update_peer_table_status_to_NPC_no_update,
# )
from diyims.general_utils import get_DTS, shutdown_query, set_controls, set_self
from diyims.path_utils import get_path_dict, get_unique_file
from diyims.logger_utils import add_log
from diyims.header_utils import ipfs_header_add
from diyims.requests_utils import execute_request
from time import sleep
from multiprocessing.managers import BaseManager
from diyims.security_utils import verify_peer_row_from_cid
from sqlalchemy.exc import NoResultFound
from sqlmodel import create_engine, Session, select, or_
from diyims.sqlmodels import Peer_Table


def peer_maintenance_main(call_stack: str) -> None:
    """
    docstring

    modify to look for changes in the peer table and the header table



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
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

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
        queue_server.register("get_publish_queue")
        queue_server.connect()
        in_bound = queue_server.get_peer_maint_queue()
        out_bound = queue_server.get_publish_queue()
    status_code = 200
    while True:
        if shutdown_query(call_stack):
            break

        statement_1 = select(Peer_Table).where(
            or_(
                Peer_Table.processing_status == "NPP",
                Peer_Table.processing_status == "PMP",
            )
        )
        with Session(engine) as session:
            results = session.exec(statement_1)
            peer_table_rows = results.all()

        # create list
        peer_list = []
        for row in peer_table_rows:
            peer_list.append(row)

        for peer in peer_list:  # peer level
            if shutdown_query(call_stack):
                break

            peer_row_CID = peer.version
            peer_ID = peer.peer_ID
            peer_type = peer.peer_type

            status_code, peer_verified, peer_row_verify_dict = verify_peer_row_from_cid(
                call_stack,
                peer_row_CID,
            )

            if peer_verified:  # the result of verifying the peer_row_cid cached in the peer row by a beacon_CID or maintenance entry from remote peer monitor. #TODO: Factor this as a function allowing for circular imports.
                peer_row_verify_dict["signature_valid"] = peer_verified
                peer_row_verify_dict["local_update_DTS"] = get_DTS()
                none_found = False
                statement_2 = select(Peer_Table).where(Peer_Table.peer_ID == peer_ID)

                try:
                    with Session(engine) as session:
                        results = session.exec(statement_2)
                        peer_table_entry = results.one()  # TODO: handle not found

                except NoResultFound:
                    none_found = True

                add_flag = False
                new_origin_value = peer_row_verify_dict[
                    "origin_update_DTS"
                ]  # potential new values

                if none_found:
                    add_flag = True
                else:
                    if peer_table_entry.origin_update_DTS is None:
                        current_origin_value = "0"
                    else:
                        current_origin_value = peer_table_entry.origin_update_DTS
                    if peer_table_entry.processing_status == "PMP":
                        # PMP comes from header chain maint
                        add_flag = True

                if add_flag:
                    peer_table_entry.processing_status = "NPC"

                    with Session(engine) as session:
                        session.add(peer_table_entry)
                        session.commit()

                    log_string = f"Peer {peer_row_verify_dict['peer_ID']}  {peer_row_verify_dict['peer_type']} {peer_type} updated."

                    if SetControlsReturn.logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=log_string,
                        )

                else:
                    # local database values
                    if current_origin_value < new_origin_value:
                        # conn, queries = set_up_sql_operations(config_dict)
                        # update_peer_table_status_to_NPC(
                        #    conn, queries, peer_row_verify_dict
                        # )
                        # conn.commit()

                        log_string = f"Peer {peer_row_verify_dict['peer_ID']}  {peer_row_verify_dict['peer_type']} {peer_type} updated."
                    else:  # this would happen if we are processing updates that occurred prior to the current entry created bt a beacon_CID
                        # conn, queries = set_up_sql_operations(config_dict)
                        # update_peer_table_status_to_NPC_no_update(
                        #    conn, queries, peer_row_verify_dict
                        # )
                        # conn.commit()

                        log_string = f"Peer {peer_row_verify_dict['peer_ID']}  {peer_row_verify_dict['peer_type']} {peer_type} updated to clear NPP."
                        if SetControlsReturn.logging_enabled:
                            add_log(
                                process=call_stack,
                                peer_type="status",
                                msg=log_string,
                            )

                # if first time for peer put out a header
                if none_found:
                    proto_path = path_dict["header_path"]
                    proto_file = path_dict["header_file"]
                    proto_file_path = get_unique_file(proto_path, proto_file)

                    param = {
                        "cid-version": 1,
                        "only-hash": "false",
                        "pin-name": "verify_peer_and_update",
                    }

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
                            msg=f"Object_CID add failed with {status_code}.",  # TODO: better error handling
                        )
                        return status_code, peer_verified

                    # object_CID = peer_row_CID
                    DTS = get_DTS()

                    if peer_type == "PP":
                        object_type = "provider_peer_entry"  # comes from want list processing one entry per peer row
                    else:
                        object_type = "remote_peer_entry"  # comes from peer monitor processing one entry per peer row

                    if not SetControlsReturn.queues_enabled:
                        mode = "init"
                    else:
                        mode = object_type

                    peer_ID = SetSelfReturn.self
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
                        if SetControlsReturn.logging_enabled:
                            add_log(
                                process=call_stack,
                                peer_type="Error",
                                msg="Peer and Update Panic.",
                            )
                        return status_code, peer_verified

            else:  # TODO: disable peer from remote monitoring
                log_string = f"Peer {peer_row_CID} signature not valid."  # How should we update the peer record???
                if SetControlsReturn.logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=log_string,
                    )

            if status_code != 200:
                if SetControlsReturn.logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="Error",
                        msg="Peer Maintenance Panic.",
                    )
                break
            if SetControlsReturn.queues_enabled:
                out_bound.put_nowait("wakeup")
                if SetControlsReturn.debug_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg="Sent wakeup.",
                    )
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


if __name__ == "__main__":
    freeze_support()
    set_start_method("spawn")

    os.environ["DIYIMS_ROAMING"] = "Roaming"
    os.environ["COMPONENT_TEST"] = "0"
    os.environ["QUEUES_ENABLED"] = "0"
    os.environ["METRICS_ENABLED"] = "1"
    os.environ["LOGGING_ENABLED"] = "1"
    os.environ["DEBUG_ENABLED"] = "1"

    peer_maintenance_main("__main__")
