import os
from datetime import datetime
from time import sleep
from sqlmodel import create_engine, Session, select, col
from sqlalchemy.exc import NoResultFound
from multiprocessing import set_start_method, freeze_support
from multiprocessing.managers import BaseManager
from queue import Empty
from diyims.requests_utils import execute_request
from diyims.database_utils import (
    refresh_peer_row_from_template,
    set_up_sql_operations,
    update_peer_table_status_WLP,
)
from diyims.general_utils import (
    get_DTS,
    get_shutdown_target,
    shutdown_query,
    set_controls,
)
from diyims.logger_utils import add_log
from diyims.config_utils import get_want_list_config_dict
from diyims.path_utils import get_path_dict
from diyims.sqlmodels import Peer_Address
from diyims.wantlist_capture_process import wantlist_capture_process_main
from diyims.class_imports import SetControlsReturn, WantlistCaptureProcessMainArgs


def wantlist_capture_submit_main(
    call_stack: str, peer_type: str
) -> None:  # each peer type runs in its own process or sequentially
    if __name__ != "__main__":
        freeze_support()
        try:
            set_start_method("spawn")
        except RuntimeError:
            pass

    call_stack = call_stack + ":wantlist_main"
    config_dict = get_want_list_config_dict()

    SetControlsReturn = set_controls(call_stack, config_dict)
    wait_before_startup = int(config_dict["wait_before_startup"])
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Waiting for {wait_before_startup} seconds before startup.",
        )
    sleep(wait_before_startup)

    add_log(
        process=call_stack,
        peer_type="status",
        msg="Want List Capture main startup.",
    )

    config_dict = get_want_list_config_dict()

    wait_seconds = int(config_dict["wait_before_startup"])

    sleep(wait_seconds)

    target_DT = get_shutdown_target(config_dict)

    max_intervals = int(config_dict["max_intervals"])
    wait_for_new_peer = 60 * int(config_dict["wait_for_new_peer_minutes"])
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Shutdown target {target_DT} or {max_intervals} intervals.",
        )
    interval_count = 1
    total_peers_processed = 0
    q_server_port = int(config_dict["q_server_port"])

    if SetControlsReturn.queues_enabled:
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")

        if peer_type == "PP":
            queue_server.register("get_want_list_queue")
            queue_server.connect()
            in_bound = queue_server.get_want_list_queue()
            # pool_workers = int(want_list_config_dict["provider_pool_workers"])
            # maxtasks = int(want_list_config_dict["provider_maxtasks"])
    # else:
    # pool_workers = int(want_list_config_dict["provider_pool_workers"])
    # maxtasks = int(want_list_config_dict["provider_maxtasks"])
    pool = None  # part of multi threading

    # with Pool(processes=pool_workers, maxtasksperchild=maxtasks) as pool:
    # used to throttle how many peers are processed concurrently

    current_DT = datetime.now()
    status_code = 200
    while target_DT > current_DT and max_intervals > interval_count:
        if shutdown_query(call_stack):
            break

        # find any available peers that were previously captured before waiting for new ones
        log_string = "Entering peer selection  for Want List processing."
        if SetControlsReturn.logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=log_string,
            )
        start_DTS = get_DTS()
        status_code, peers_processed = capture_wantlist_peers(
            call_stack,
            config_dict,
            peer_type,
            pool,
            SetControlsReturn,
        )
        if SetControlsReturn.metrics_enabled:
            stop_DTS = get_DTS()
            start = datetime.fromisoformat(start_DTS)
            stop = datetime.fromisoformat(stop_DTS)
            duration = stop - start
            add_log(
                process=call_stack,
                peer_type="status",
                msg=f"capture_wantlist_peers completed in {duration} seconds with {status_code}.",
            )

        if shutdown_query(call_stack):
            break

        if status_code != 200:
            if SetControlsReturn.logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=f"Capture wantlist peers failed with {status_code}.",
                )
        else:
            total_peers_processed += peers_processed
            log_string = f"For interval {interval_count}, {peers_processed} peers processed for {peer_type} type peers submitted for Want List processing."
            if SetControlsReturn.logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=log_string,
                )
            if SetControlsReturn.logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=f"Entering Interval wait for {wait_for_new_peer} seconds.",
                )
            try:
                if SetControlsReturn.queues_enabled:
                    log_string = in_bound.get(timeout=wait_for_new_peer)
                    # comes from peer capture process or shutdown

                else:
                    sleep(wait_for_new_peer)

                if shutdown_query(call_stack):
                    break
            except Empty:
                if shutdown_query(call_stack):
                    break

            interval_count += 1
            current_DT = datetime.now()

    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"{total_peers_processed} {peer_type} total peers submitted for Want List  processing.",
        )

    add_log(
        process=call_stack,
        peer_type="status",
        msg=f"Want List complete with {status_code}.",
    )
    return


def capture_wantlist_peers(
    call_stack: str,
    want_list_config_dict: dict,
    peer_type: str,
    pool: str,
    SetControlsReturn: SetControlsReturn,
) -> tuple[str, int]:
    call_stack = call_stack + ":capture_wantlist_peers"
    peers_processed = 0

    conn, queries = set_up_sql_operations(want_list_config_dict)  # + 1

    rows_of_peers = queries.select_peers_by_peer_type_status(  # NOTE: where status is WLR set by peer capture and reset by want list capture
        conn, peer_type=peer_type
    )
    list_of_peers = []
    if rows_of_peers is not None:
        for peer_row in rows_of_peers:
            list_of_peers.append(peer_row)
    conn.close()
    status_code = 200
    for peer_row in list_of_peers:
        if shutdown_query(call_stack):
            break

        peer_ID = peer_row["peer_ID"]
        status_code, peer_connected = peer_connect(
            call_stack,
            peer_ID,
            SetControlsReturn,
        )  # stats code ?
        status_code = 200
        if peer_connected:
            peer_table_dict = refresh_peer_row_from_template()
            peer_table_dict["peer_ID"] = peer_row["peer_ID"]
            peer_table_dict["peer_type"] = peer_row["peer_type"]
            # peer_table_dict["processing_status"] = (
            #    "WLP"  # suppress resubmission by WLR -> WLP
            # )
            peer_table_dict["local_update_DTS"] = get_DTS()
            conn, queries = set_up_sql_operations(want_list_config_dict)  # + 1

            update_peer_table_status_WLP(conn, queries, peer_table_dict)
            conn.commit()
            conn.close()

            start_DTS = get_DTS()
            Args = WantlistCaptureProcessMainArgs(
                call_stack=call_stack,
                want_list_config_dict=want_list_config_dict,
                peer_table_dict=peer_table_dict,
                set_controls_return=SetControlsReturn,
            )

            if SetControlsReturn.single_thread:  # partial implementation
                status_code = wantlist_capture_process_main(Args)
                if SetControlsReturn.metrics_enabled:
                    stop_DTS = get_DTS()
                    start = datetime.fromisoformat(start_DTS)
                    stop = datetime.fromisoformat(stop_DTS)
                    duration = stop - start
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=f"wantlist_capture_process_main completed in {duration} seconds with {status_code}.",
                    )

                if status_code != 200:
                    if SetControlsReturn.logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=f"Want List capture process failed for {peer_ID}.",
                        )
                else:
                    peers_processed += 1
            else:
                pool.apply_async(wantlist_capture_process_main, args=(Args))
                log_string = f"Peer id {peer_ID} submitted."
                if SetControlsReturn.logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=log_string,
                    )
                peers_processed += 1

        else:
            status_code = 200
            log_string = f"Peer not connected for {peer_ID}"
            if SetControlsReturn.logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=log_string,
                )

    return status_code, peers_processed


def peer_connect(
    call_stack: str,
    peer_ID: str,
    SetControlsReturn: SetControlsReturn,
) -> bool:
    call_stack = call_stack + ":peer_connect"
    peer_connected = False
    status_code = 200
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"

    # engine = create_engine(db_url, echo=True)
    engine = create_engine(db_url, echo=False, connect_args={"timeout": 120})

    statement = (
        select(Peer_Address)
        .where(Peer_Address.peer_ID == peer_ID)
        .where(Peer_Address.in_use == "1")
    )

    with Session(engine) as session:
        try:
            results = session.exec(statement).one()
            peer_connected = True
            status_code = 200
            return status_code, peer_connected
        except NoResultFound:
            peer_connected = False

    if peer_connected:
        pass

    else:
        statement = (
            select(Peer_Address)
            .where(Peer_Address.peer_ID == peer_ID)
            .where(Peer_Address.available == "1")
            .order_by(col(Peer_Address.insert_DTS).desc())
        )
        with Session(engine) as session:
            results = session.exec(statement).all()  # TODO: put this in a list

        for peer_address in results:
            param = {"arg": peer_address.multiaddress}

            response, status_code, response_dict = execute_request(
                url_key="connect",
                param=param,
                call_stack=call_stack,
            )
            # if successfully connected update address used
            if status_code == 200:
                peer_connected = True
                statement = select(Peer_Address).where(
                    Peer_Address.address_string == peer_address.address_string
                )
                with Session(engine) as session:
                    results = session.exec(statement)
                    address = results.one()

                    address.in_use = True
                    address.connect_DTS = get_DTS()

                    session.add(address)
                    session.commit()

                response, status_code, response_dict = execute_request(
                    url_key="peering_add",
                    param=param,
                    call_stack=call_stack,
                )

                if status_code == 200:
                    statement = select(Peer_Address).where(
                        Peer_Address.address_string == peer_address.address_string
                    )
                    with Session(engine) as session:
                        results = session.exec(statement)
                        address = results.one()

                        address.peering_add_DTS = get_DTS()

                        session.add(address)
                        session.commit()

                else:
                    status_code = 200
                    if SetControlsReturn.logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=f"{peer_ID} peering add failed.",
                        )
            elif status_code == 500:
                peer_connected = False
            else:
                peer_connected = False
                if SetControlsReturn.logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=f"Peer connect failed for {peer_ID}.",
                    )

            if peer_connected:
                break  # don't need any more connections

    return status_code, peer_connected


if __name__ == "__main__":
    freeze_support()
    set_start_method("spawn")

    os.environ["DIYIMS_ROAMING"] = "RoamingDev"
    os.environ["COMPONENT_TEST"] = "0"
    os.environ["QUEUES_ENABLED"] = "0"
    os.environ["LOGGING_ENABLED"] = "1"

    wantlist_capture_submit_main("__main__", "PP")
