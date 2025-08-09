"""
The beacon length should stand out from regular traffic.(2)
This will be? measured by capturing stats from the swarm (1)
The stats captured will also be used to create timing information (3)
    will be used to load test the system as well as test for beacon distortion.
"""

from queue import Empty

import os
import json
from diyims.requests_utils import execute_request
from datetime import datetime
from time import sleep
from multiprocessing.managers import BaseManager
from sqlmodel import create_engine, Session, select, col
from diyims.general_utils import get_DTS, get_shutdown_target
from diyims.path_utils import get_path_dict, get_unique_file
from diyims.config_utils import get_beacon_config_dict

from diyims.sqlmodels import Clean_Up, Shutdown, Header_Table


def beacon_main(call_stack: str) -> None:
    try:
        component_test = os.environ["COMPONENT_TEST"]
    except ValueError:
        component_test = False

    call_stack = call_stack + ":beacon_main"
    config_dict = get_beacon_config_dict()
    logging_enabled = config_dict["long_period_seconds"]  # TODO: official entry

    wait_before_startup = int(config_dict["wait_before_startup"])
    add_log(
        process=call_stack,
        peer_type="status",
        msg=f"Waiting for {wait_before_startup} seconds before startup.",
    )
    sleep(wait_before_startup)
    add_log(
        process=call_stack,
        peer_type="status",
        msg="Beacon startup.",
    )

    response, status_code, response_dict = execute_request(
        url_key="id",
        config_dict=config_dict,
        call_stack=call_stack,
        http_500_ignore=False,
    )
    self = response_dict["ID"]

    target_DT = get_shutdown_target(config_dict)
    current_DT = datetime.now()

    max_intervals = int(config_dict["max_intervals"])
    add_log(
        process=call_stack,
        peer_type="debug",
        msg=f"Shutdown target {target_DT} or {max_intervals} intervals.",
    )

    beacon_interval = 0

    if not component_test:
        q_server_port = int(config_dict["q_server_port"])
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
        queue_server.register("get_satisfy_queue")
        queue_server.register("get_beacon_queue")
        queue_server.connect()
        out_bound_ping = queue_server.get_satisfy_queue()
        in_bound_wait = queue_server.get_beacon_queue()

    while target_DT > current_DT and beacon_interval < max_intervals:
        for _ in range(int(config_dict["number_of_periods"])):
            if shutdown_query(call_stack):
                break

            beacon_CID, want_item_file = create_beacon_CID(
                call_stack,
                config_dict,
                self,
            )

            if not component_test:
                out_bound_ping.put_nowait("ping")
                try:
                    in_bound_wait.get()
                except Empty:
                    pass

            if shutdown_query(call_stack):
                break

            if logging_enabled:
                if not component_test:
                    add_log(
                        process=call_stack,
                        peer_type="debug",
                        msg="ping",
                    )

            flash_beacon(
                call_stack,
                config_dict,
                beacon_CID,
            )

            if shutdown_query(call_stack):
                break

        beacon_interval += 1
        current_DT = datetime.now()

        if shutdown_query(call_stack):
            break

    if not component_test:
        out_bound_ping.put_nowait("ping")

    add_log(
        process=call_stack,
        peer_type="debug",
        msg="Beacon shutdown.",
    )
    return


def create_beacon_CID(
    call_stack: str,
    config_dict: str,
    self: str,
) -> tuple[str, str]:
    """This function creates an objectCID and stores a reference in the Clean_Up table.

    Args:
        call_stack (str): _description_
        config_dict (str): _description_
        self (str): _description_

    Returns:
        tuple[str, str]: _description_
    """
    call_stack = call_stack + ":create_beacon_CID"
    logging_enabled = config_dict["long_period_seconds"]

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    statement = (
        select(Header_Table)
        .where(Header_Table.peer_ID == self)
        .where(Header_Table.object_type == "local_peer_entry")
        .order_by(col(Header_Table.insert_DTS).asc())
    )
    with Session(engine) as session:
        results = session.exec(statement)
        header_row = results.first()

    want_item_dict = {}
    want_item_dict["peer_row_CID"] = header_row["object_CID"]
    want_item_dict["DTS"] = get_DTS()

    want_item_path = path_dict["want_item_path"]
    proto_item_file = path_dict["want_item_file"]
    want_item_file = get_unique_file(want_item_path, proto_item_file)

    with open(want_item_file, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(want_item_dict, write_file, indent=4)

    f = open(want_item_file, "rb")
    file = {"file": f}
    param = {"only-hash": "true", "pin": "false", "cid-version": 1}
    response, status_code, response_dict = execute_request(
        url_key="add",
        config_dict=config_dict,
        param=param,
        file=file,
        call_stack=call_stack,
        http_500_ignore=False,
    )
    f.close()

    beacon_CID = response_dict["Hash"]  # TODO: handle 700

    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="debug",
            msg=f"Create {beacon_CID}",
        )

    clean_up_entry = Clean_Up(
        DTS=get_DTS(), want_item_file=want_item_file, beacon_CID=beacon_CID
    )

    with Session(engine) as session:
        session.add(clean_up_entry)
        session.commit()

    return tuple[beacon_CID, want_item_file]


def flash_beacon(
    call_stack: str,
    config_dict: str,
    beacon_CID: str,
) -> None:
    """This function waits for the beacon_CID to become available, creating an entry in the want list.

    Args:
        call_stack (str): _description_
        logger (str): _description_
        config_dict (str): _description_
        beacon_CID (str): _description_
    """
    call_stack = call_stack + ":flash_beacon"
    logging_enabled = config_dict["long_period_seconds"]

    try:
        component_test = os.environ["COMPONENT_TEST"]
    except ValueError:
        component_test = False

    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Flash on.",
        )

    if not component_test:
        execute_request(
            url_key="get",
            config_dict=config_dict,
            param={
                "arg": beacon_CID,
            },
            call_stack=call_stack,
        )
    else:
        sleep(10)

    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Flash off.",
        )

    return


def satisfy_main(call_stack: str) -> None:
    """These functions use the want_item_file created by the beacon process to satisfy the flash_beacon wait condition.
    main is responsible for control functions. beacon is responsible for the mechanics.


    Args:
        call_stack (str): _description_
    """
    try:
        component_test = os.environ["COMPONENT_TEST"]
    except ValueError:
        component_test = False

    call_stack = call_stack + ":satisfy_main"
    config_dict = get_beacon_config_dict()
    logging_enabled = config_dict["long_period_seconds"]

    wait_before_startup = int(config_dict["wait_before_startup"])
    add_log(
        process=call_stack,
        peer_type="status",
        msg=f"Waiting for {wait_before_startup} seconds before startup.",
    )
    sleep(wait_before_startup)
    add_log(
        process=call_stack,
        peer_type="status",
        msg="Satisfy startup",
    )

    if not component_test:
        q_server_port = int(config_dict["q_server_port"])
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
        queue_server.register("get_satisfy_queue")
        queue_server.register("get_beacon_queue")
        queue_server.connect()
        in_bound_wait = queue_server.get_satisfy_queue()
        out_bound_ping = queue_server.get_beacon_queue()

        wakeup = in_bound_wait.get()
    else:
        wakeup = "ping"

    while wakeup == "ping":
        if not component_test:
            out_bound_ping.put_nowait("ping")
            try:
                wakeup = in_bound_wait.get(
                    timeout=int(config_dict["short_period_seconds"])
                )
            except Empty:
                pass

        satisfy_beacon(call_stack, config_dict, logging_enabled)

        if shutdown_query(call_stack):
            break

    if not component_test:
        out_bound_ping.put_nowait("ping")

    add_log(
        process=call_stack,
        peer_type="status",
        msg="Satisfy shutdown.",
    )

    return


def satisfy_beacon(
    call_stack: str,
    config_dict: str,
) -> None:
    """This function uses the want_item_file created by the beacon process to satisfy the flash_beacon wait condition.

    Args:
        call_stack (str): _description_
        config_dict (str): _description_
    """
    call_stack = call_stack + ":satisfy_beacon"
    logging_enabled = config_dict["long_period_seconds"]
    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    statement = (
        select(Clean_Up).order_by(
            col(Clean_Up.DTS).desc()
        )  # most recent entry assumed to be the entry being waited on. #TODO: make this explicit.
    )
    with Session(engine) as session:
        results = session.exec(statement)
        clean_up = results.first()

    want_item_file = clean_up.want_item_file
    f = open(want_item_file, "rb")
    file = {"file": f}

    param = {"only-hash": "false", "pin": "true", "cid-version": 1}
    execute_request(
        url_key="add",
        config_dict=config_dict,
        param=param,
        file=file,
        call_stack=call_stack,
        http_500_ignore=False,  # TODO: 700
    )
    f.close()
    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="debug",
            msg=f"Satisfy {want_item_file}",
        )

    return


def shutdown_query(call_stack: str) -> bool:
    call_stack = call_stack + ":shutdown_query"

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    statement = select(Shutdown)
    with Session(engine) as session:
        results = session.exec(statement)
        shutdown = results.one()

    return bool(shutdown.enabled)


def add_log(process: str, peer_type: str, msg: str):
    import psutil
    from diyims.general_utils import get_DTS
    from diyims.sqlmodels import Log
    from diyims.path_utils import get_path_dict
    from sqlmodel import Session, create_engine

    p = psutil.Process()
    pid = p.pid

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"

    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    log_entry = Log(
        DTS=get_DTS(), process=process, pid=pid, peer_type=peer_type, msg=msg
    )

    with Session(engine) as session:
        session.add(log_entry)
        session.commit()
