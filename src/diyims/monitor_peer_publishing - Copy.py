from datetime import datetime
from time import sleep
import os
from queue import Empty
from multiprocessing.managers import BaseManager
from multiprocessing import set_start_method, freeze_support

# from sqlite3 import IntegrityError
from diyims.config_utils import get_peer_monitor_config_dict

# from diyims.database_utils import (
# insert_peer_row,
# set_up_sql_operations,
# insert_header_row,
# refresh_peer_row_from_template,
# update_peer_table_status_to_PMP,
# update_peer_table_status_to_PMP_type_PR,
# add_header_chain_status_entry,
# )
from diyims.requests_utils import execute_request
from diyims.logger_utils import add_log

# from diyims.ipfs_utils import unpack_peer_row_from_cid
from diyims.general_utils import get_DTS, shutdown_query, set_controls
from diyims.path_utils import get_path_dict
from sqlmodel import create_engine, Session, select
from diyims.sqlmodels import Peer_Table, Header_Table
from diyims.header_utils import header_chain_maint
from sqlalchemy.exc import NoResultFound
# from rich import print


def monitor_peer_publishing_main(call_stack: str) -> None:
    """
    docstring
    """
    if __name__ != "__main__":
        freeze_support()
        try:
            set_start_method("spawn")
        except RuntimeError:
            pass
    call_stack = call_stack + ":monitor_peer_publishing"
    config_dict = get_peer_monitor_config_dict()
    wait_time = int(config_dict["wait_time"])
    path_dict = get_path_dict()
    SetControlsReturn = set_controls(call_stack, config_dict)

    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    wait_before_startup = int(config_dict["wait_before_startup"])
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Waiting for {wait_before_startup} seconds before startup.",
        )
    sleep(wait_before_startup)
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Remote Peer Monitor startup.",
        )

    status_code = 200

    if SetControlsReturn.queues_enabled:
        q_server_port = int(config_dict["q_server_port"])
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
        queue_server.register("get_peer_maint_queue")
        queue_server.register("get_remote_monitor_queue")
        queue_server.connect()
        out_bound = queue_server.get_peer_maint_queue()
        in_bound = queue_server.get_remote_monitor_queue()
    else:
        out_bound = None

    statement_1 = (
        select(Peer_Table)
        .where(Peer_Table.signature_valid == 1)
        .where(Peer_Table.disabled == 0)
    )

    while True:
        peer_list = []
        with Session(engine) as session:
            results = session.exec(statement_1).all()
            peer_rows = results
            for peer in peer_rows:
                peer_list.append(peer)

        for peer in peer_list:
            if shutdown_query(call_stack):
                break

            if (  # select peer to process. ignore the local peer and select only peers that have been processed
                peer.peer_type != "LP"
                and peer.processing_status
                == "NPC"  # or WLW or WLR#TODO: accept expanded list of conditions
            ):  # this single threads updates from a  remote peer
                ipns_path = "/ipns/" + peer.IPNS_name

                start_DTS = get_DTS()
                param = {"arg": ipns_path}
                response, status_code, response_dict = execute_request(
                    url_key="resolve",
                    param=param,
                    call_stack=call_stack,
                    connect_retries=0,  # disable peer on first resolve failure #TODO: when to retry?????
                    http_500_ignore=False,
                )
                if SetControlsReturn.metrics_enabled:
                    stop_DTS = get_DTS()
                    start = datetime.fromisoformat(start_DTS)
                    stop = datetime.fromisoformat(stop_DTS)
                    duration = stop - start
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=f"Resolve completed in {duration} seconds with {status_code}.",
                    )

                if status_code != 200:
                    statement_2 = select(Peer_Table).where(
                        Peer_Table.peer_ID == peer.peer_ID
                    )
                    with Session(engine) as session:
                        results = session.exec(statement_2)
                        current_peer = results.one()
                        current_peer.disabled = 1
                        session.add(current_peer)
                        session.commit()
                    if SetControlsReturn.logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="Error",
                            msg=f"Resolve for {peer.peer_ID} failed and peer disabled.",
                        )

                if status_code == 200:
                    peer_ID = peer.peer_ID

                    # conn, queries = set_up_sql_operations(config_dict)
                    ipfs_sourced_header_CID = response_dict["Path"][
                        6:
                    ]  # header cid in publish
                    # last published cid that was processed
                    # db_header_row = queries.select_last_header(conn, peer_ID=peer_ID)
                    # conn.close()
                    statement = (
                        select(Header_Table)
                        .where(Header_Table.peer_ID == peer_ID)
                        .where(Header_Table.header_CID == ipfs_sourced_header_CID)
                        # .order_by(col(Header_Table.insert_DTS).desc())
                    )
                    # header_dict = {}
                    with Session(engine) as session:
                        results = session.exec(statement)

                        try:
                            results.one()
                            header_not_found = False
                        except NoResultFound:
                            header_not_found = True

                    if (
                        header_not_found  # TODO: change logic to support NoResults logic
                    ):  # we have not seen this peer before this costs one db read in exchange for one extra cat with an insert exception
                        start_DTS = get_DTS()
                        status_code = header_chain_maint(
                            call_stack,
                            ipfs_sourced_header_CID,
                            config_dict,
                            out_bound,
                            peer_ID,
                            SetControlsReturn.logging_enabled,
                            SetControlsReturn.queues_enabled,
                            SetControlsReturn.debug_enabled,
                        )  # add one or more headers
                        if SetControlsReturn.metrics_enabled:
                            stop_DTS = get_DTS()
                            start = datetime.fromisoformat(start_DTS)
                            stop = datetime.fromisoformat(stop_DTS)
                            duration = stop - start
                            add_log(
                                process=call_stack,
                                peer_type="status",
                                msg=f"header_chain_maint for {peer.peer_ID} completed in {duration} seconds with {status_code}.",
                            )
                        if status_code != 200:
                            add_log(
                                process=call_stack,
                                peer_type="Error",
                                msg=f"Remote Monitor Panic {status_code}.",
                            )
                        # out_bound.put_nowait("wake up")
                    else:
                        pass
                    """
                        most_recent_db_header = header_row.header_CID
                        # if we have a null cid at head of chain we should only process current entries
                        # if we have a gap whe should process the current entry and follow the chain to see if we can fill the gap
                        # if we find the null entry we should delete any gap entry ??
                        if (
                            most_recent_db_header == ipfs_sourced_header_CID
                        ):  # nothing new #TODO: we should check for an existing gap and retry if there is one, could be expensive
                            pass
                            # print(f"no new entries for {peer_ID}")
                        else:
                            start_DTS = get_DTS()
                            status_code = header_chain_maint(
                                call_stack,
                                ipfs_sourced_header_CID,
                                config_dict,
                                out_bound,
                                peer_ID,
                                SetControlsReturn.logging_enabled,
                                SetControlsReturn.queues_enabled,
                                SetControlsReturn.debug_enabled,
                            )  # add one or more headers
                            if SetControlsReturn.metrics_enabled:
                                stop_DTS = get_DTS()
                                start = datetime.fromisoformat(start_DTS)
                                stop = datetime.fromisoformat(stop_DTS)
                                duration = stop - start
                                add_log(
                                    process=call_stack,
                                    peer_type="status",
                                    msg=f"header_chain_maint for {peer.peer_ID} completed in {duration} seconds with {status_code}.",
                                )

                            if status_code != 200:
                                add_log(
                                    process=call_stack,
                                    peer_type="Error",
                                    msg=f"Remote Monitor Panic with {status_code}.",
                                )
                    """
                    # out_bound.put_nowait("wake up")
        try:  # peer list completed
            if SetControlsReturn.queues_enabled:
                in_bound.get(
                    timeout=wait_time  # config value
                )  # comes from peer capture process
                if shutdown_query(call_stack):
                    break

            else:
                sleep(wait_time)  # config value
                if shutdown_query(call_stack):
                    break

        except Empty:
            if shutdown_query(call_stack):
                break

    add_log(
        process=call_stack,
        peer_type="status",
        msg=f"Remote Peer Monitor complete with {status_code}.",
    )

    return


if __name__ == "__main__":
    import os
    from multiprocessing import set_start_method, freeze_support

    freeze_support()
    set_start_method("spawn")

    os.environ["COMPONENT_TEST"] = "1"
    os.environ["QUEUES_ENABLED"] = "0"
    monitor_peer_publishing_main("__main__")
