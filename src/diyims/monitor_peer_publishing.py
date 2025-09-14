from datetime import datetime
from time import sleep
import os
from queue import Empty
from multiprocessing.managers import BaseManager
from multiprocessing import set_start_method, freeze_support
from sqlite3 import IntegrityError
from diyims.config_utils import get_peer_monitor_config_dict
from diyims.database_utils import (
    insert_peer_row,
    set_up_sql_operations,
    insert_header_row,
    refresh_peer_row_from_template,
    update_peer_table_status_to_PMP,
    update_peer_table_status_to_PMP_type_PR,
    add_header_chain_status_entry,
)
from diyims.requests_utils import execute_request
from diyims.logger_utils import add_log
from diyims.ipfs_utils import unpack_peer_row_from_cid
from diyims.general_utils import get_DTS, shutdown_query, set_controls
from diyims.path_utils import get_path_dict
from sqlmodel import create_engine, Session, select
from diyims.sqlmodels import Peer_Table
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

            if (
                peer.peer_type != "LP" and peer.processing_status == "NPC"
            ):  # this single threads updates to a  remote peer
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

                    conn, queries = set_up_sql_operations(config_dict)
                    ipfs_header_CID = response_dict["Path"][6:]  # header cid in publish
                    # last published cid that was processed
                    db_header_row = queries.select_last_header(conn, peer_ID=peer_ID)
                    conn.close()
                    if (
                        db_header_row is None
                    ):  # we have not seen this peer before this costs one db read in exchange for one extra cat with an insert exception
                        start_DTS = get_DTS()
                        status_code = header_chain_maint(
                            call_stack,
                            ipfs_header_CID,
                            config_dict,
                            out_bound,
                            peer_ID,
                            SetControlsReturn.logging_enabled,
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
                    else:
                        most_recent_db_header = db_header_row["header_CID"]
                        # if we have a null cid a head of chain we should only process current entries
                        # if we have a gap whe should process the current entry and follow the chain to see if we can fill the gap
                        # if we find the null entry we should delete any gap entry ??
                        if (
                            most_recent_db_header == ipfs_header_CID
                        ):  # nothing new #TODO: we should check for an existing gap and retry if there is one, could be expensive
                            pass
                            # print(f"no new entries for {peer_ID}")
                        else:
                            start_DTS = get_DTS()
                            status_code = header_chain_maint(
                                call_stack,
                                ipfs_header_CID,
                                config_dict,
                                out_bound,
                                peer_ID,
                                SetControlsReturn.logging_enabled,
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
                            # this assumes an in order arrival sequence dht delivers a best value as the most current

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


def header_chain_maint(
    call_stack,
    ipfs_header_CID,
    config_dict,
    out_bound,
    peer_ID,
    logging_enabled,
):
    """
    docstring
    """
    call_stack = call_stack + ":header_chain_maint"
    status_code = 200
    # ipfs_config_dict = get_ipfs_config_dict()
    while True:
        start_DTS = get_DTS()
        # ipfs_header_CID =  "QmbY1Utuz753VwtQGyBvirB5Hgn8wouaRZ1xzBa99KaMRB"
        ipfs_path = "/ipfs/" + ipfs_header_CID

        param = {"arg": ipfs_path}
        response, status_code, response_dict = execute_request(
            url_key="cat",
            param=param,
            timeout=(3.05, 122),  # avoid timeouts
            call_stack=call_stack,
            http_500_ignore=False,
        )
        if status_code != 200:  # TODO: disable peer after ????
            header_chain_status_dict = {}
            header_chain_status_dict["insert_DTS"] = get_DTS()
            header_chain_status_dict["peer_ID"] = peer_ID
            header_chain_status_dict["missing_header_CID"] = ipfs_header_CID
            header_chain_status_dict["message"] = "missing header"
            conn, queries = set_up_sql_operations(config_dict)
            try:
                add_header_chain_status_entry(
                    conn,
                    queries,
                    header_chain_status_dict,
                )
                conn.commit()
                conn.close()
            except IntegrityError:
                conn.rollback()
                conn.close()
                pass

            break  # log chain broken

        stop_DTS = get_DTS()
        start = datetime.fromisoformat(start_DTS)
        stop = datetime.fromisoformat(stop_DTS)
        duration = stop - start
        msg = f"In {duration} CAT {response_dict}."
        if logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=msg,
            )

        object_type = response_dict["object_type"]
        object_CID = response_dict["object_CID"]

        if object_type == "local_peer_entry" or object_type == "provider_peer_entry":
            status_code, remote_peer_row_dict = unpack_peer_row_from_cid(
                call_stack, object_CID, config_dict
            )
            # TODO: disable peer if != 200

            proto_remote_peer_row_dict = refresh_peer_row_from_template()
            proto_remote_peer_row_dict["peer_ID"] = remote_peer_row_dict["peer_ID"]
            proto_remote_peer_row_dict["peer_type"] = "RP"
            proto_remote_peer_row_dict["version"] = object_CID
            proto_remote_peer_row_dict["local_update_DTS"] = get_DTS()
            proto_remote_peer_row_dict["processing_status"] = "PMP"
            proto_remote_peer_row_dict["disabled"] = "0"

            conn, queries = set_up_sql_operations(config_dict)
            try:
                insert_peer_row(conn, queries, proto_remote_peer_row_dict)
                conn.commit()
                conn.close()
                # out_bound.put_nowait("wake up")

                msg = f"Peer {remote_peer_row_dict['peer_ID']} added."
                if logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=msg,
                    )

            except IntegrityError:
                conn.rollback()
                conn.close()
                if object_type == "local_peer_entry":
                    # this will trigger peer maint by npp without change anything but the version, etc.
                    conn, queries = set_up_sql_operations(config_dict)
                    update_peer_table_status_to_PMP(
                        conn, queries, proto_remote_peer_row_dict
                    )
                    conn.commit()
                    conn.close()
                    # out_bound.put_nowait("wake up")

                    msg = f"Peer {remote_peer_row_dict['peer_ID']} updated."
                    if logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=msg,
                        )
                else:
                    # peer_row_dict = select_peer_table_entry_by_key(conn, queries, remote_peer_row_dict)

                    # from PP to PR to indicate an overlay if not already NPP or NPC  triggers a provider source change
                    proto_remote_peer_row_dict["peer_type"] = "PR"
                    conn, queries = set_up_sql_operations(config_dict)
                    update_peer_table_status_to_PMP_type_PR(
                        conn, queries, proto_remote_peer_row_dict
                    )
                    conn.commit()
                    conn.close()
                    # out_bound.put_nowait("wake up")

                    msg = f"Peer {remote_peer_row_dict['peer_ID']} updated."
                    if logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=msg,
                        )

        conn, queries = set_up_sql_operations(config_dict)
        try:  # this method adds one extra cat to the process
            response_dict["processing_status"] = get_DTS()
            insert_header_row(conn, queries, response_dict, ipfs_header_CID)
            conn.commit()
            conn.close()

        except IntegrityError:
            # pass will correct missing db components ########this leg shouldn't happen
            conn.rollback()
            conn.close()
            break

        # this method eliminates the cat  abd insert exception and uses a db read instead

        ipfs_header_CID = response_dict["prior_header_CID"]

        if ipfs_header_CID == "null":
            header_chain_status_dict = {}
            header_chain_status_dict["insert_DTS"] = get_DTS()
            header_chain_status_dict["peer_ID"] = peer_ID
            header_chain_status_dict["missing_header_CID"] = ipfs_header_CID
            header_chain_status_dict["message"] = "Root header found"
            conn, queries = set_up_sql_operations(config_dict)
            add_header_chain_status_entry(
                conn,
                queries,
                header_chain_status_dict,
            )
            conn.commit()
            conn.close()
            break  # log chain complete
        conn, queries = set_up_sql_operations(config_dict)
        db_header_row = queries.select_header_CID(  # TODO: change to db function
            conn, header_CID=ipfs_header_CID
        )  # test for next entry
        conn.close()
        if (
            db_header_row is not None
        ):  # If not missing, this will add to the chain until the prior is null
            break

    # a missing cid ot time out currently goes here
    # a nieve assumption would be to treat it as a missing cid

    # log gap
    return status_code


if __name__ == "__main__":
    import os
    from multiprocessing import set_start_method, freeze_support

    freeze_support()
    set_start_method("spawn")

    os.environ["COMPONENT_TEST"] = "1"
    os.environ["QUEUES_ENABLED"] = "0"
    monitor_peer_publishing_main("__main__")
