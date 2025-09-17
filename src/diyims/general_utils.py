import sqlite3
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse
import os

# from dataclasses import dataclass
import aiosql
from pathlib import Path
from sqlmodel import create_engine, Session, select
from diyims.sqlmodels import Clean_Up, Peer_Table
from sqlalchemy.exc import NoResultFound
from diyims.class_imports import SetControlsReturn, SetSelfReturn
from diyims.path_utils import get_path_dict
from diyims.py_version_dep import get_sql_str
from diyims.config_utils import (
    get_clean_up_config_dict,
    get_shutdown_config_dict,
    get_scheduler_config_dict,
)
from diyims.logger_utils import add_log
from diyims.database_utils import (
    delete_want_list_table_rows_by_date,
    set_up_sql_operations,
    refresh_clean_up_dict,
    set_up_sql_operations_list,
    delete_log_rows_by_date,
    update_shutdown_enabled_1,
)
from diyims.requests_utils import execute_request


def get_agent() -> str:
    agent = "0.0.0a145"  # NOTE: How to extract at run time

    return agent


def exec_uvicorn(roaming: str) -> None:
    import os
    import uvicorn

    os.environ["DIYIMS_ROAMING"] = str(roaming)
    uvicorn.run("diyims.fastapi_app:myapp", host="0.0.0.0", port=8000)

    return


def exec_fastapi(roaming: str) -> None:
    import os
    import uvicorn

    os.environ["DIYIMS_ROAMING"] = str(roaming)
    uvicorn.run("diyims.fastapi_app:myapp", host="127.0.0.1", port=8001)

    return


def get_network_name() -> str:
    path_dict = get_path_dict()
    sql_str = get_sql_str()
    queries = aiosql.from_str(sql_str, "sqlite3")
    connect_path = path_dict["db_file"]
    conn = sqlite3.connect(connect_path)
    conn.row_factory = sqlite3.Row
    query_row = queries.select_network_name(conn)
    network_name = query_row["network_name"]
    conn.close()
    return network_name


def set_controls(call_stack: str, config_dict: dict) -> SetControlsReturn:
    """
    set_controls _summary_

    _extended_summary_

    Arguments:
        call_stack {str} -- _description_
        config_dict {dict} -- _description_

    Returns:
        tuple[bool, bool, bool, bool, bool] -- _description_
    """

    queues_enabled = bool(int(config_dict["queues_enabled"]))
    try:
        queues_enabled = bool(int(os.environ["QUEUES_ENABLED"]))
    except KeyError:
        pass

    try:
        component_test = bool(int(os.environ["COMPONENT_TEST"]))
    except KeyError:
        component_test = False

    logging_enabled = bool(int(config_dict["logging_enabled"]))
    try:
        logging_enabled = bool(int(os.environ["LOGGING_ENABLED"]))
    except KeyError:
        pass

    debug_enabled = bool(int(config_dict["debug_enabled"]))
    try:
        debug_enabled = bool(int(os.environ["DEBUG_ENABLED"]))
    except KeyError:
        pass

    try:
        single_thread = bool(int(config_dict["single_thread"]))
    except KeyError:
        single_thread = bool(1)
    try:
        single_thread = bool(int(os.environ["SINGLE_THREAD"]))
    except KeyError:
        pass

    try:
        metrics_enabled = bool(int(config_dict["metrics_enabled"]))
    except KeyError:
        metrics_enabled = bool(1)
    try:
        metrics_enabled = bool(int(os.environ["METRICS_ENABLED"]))
    except KeyError:
        pass

    set_controls_return = SetControlsReturn()
    set_controls_return.component_test = component_test
    set_controls_return.debug_enabled = debug_enabled
    set_controls_return.logging_enabled = logging_enabled
    set_controls_return.queues_enabled = queues_enabled
    set_controls_return.single_thread = single_thread
    set_controls_return.metrics_enabled = metrics_enabled

    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Logging enabled",
        )

        if queues_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Queues enabled",
            )
        else:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Queues disabled",
            )

        if debug_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Debug enabled",
            )
        else:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Debug disabled",
            )

        if component_test:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Component Test enabled",
            )
        else:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Component Test disabled",
            )

        if single_thread:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Single Thread enabled",
            )
        else:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Single Thread disabled",
            )

        if metrics_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Metrics enabled",
            )
        else:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Metrics disabled",
            )

    return set_controls_return


def set_self() -> SetSelfReturn:
    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    statement_1 = select(Peer_Table).where(Peer_Table.peer_type == "LP")

    with Session(engine) as session:
        results = session.exec(statement_1)
        current_peer = results.one()
        self = current_peer.peer_ID
        IPNS_name = current_peer.IPNS_name

    set_self = SetSelfReturn(self=self, IPNS_name=IPNS_name)
    return set_self


def get_DTS() -> str:
    """Generates an iso6??? formatted UTC time string
    suitable for timestamp use. if the value is to be passed to a url query string the
    + in the iso format must be replaced with a %2B

    Returns:
        str: UTC datetime.now() in an iso format
    """
    DTS = datetime.now(timezone.utc).isoformat()
    # DTSTemp = DTS.replace('+', '%2B')

    return DTS


def get_shutdown_target(config_dict: dict) -> str:
    current_date = datetime.today()
    shutdown_time = config_dict["shutdown_time"]
    if shutdown_time == "99:99:99":
        shutdown_time = str(current_date + timedelta(weeks=10))
    target_DT = parse(shutdown_time, default=current_date)

    return target_DT


def shutdown_cmd(call_stack):
    from multiprocessing.managers import BaseManager

    call_stack = call_stack + ":shutdown_cmd"
    config_dict = get_shutdown_config_dict()
    queue_dict = get_scheduler_config_dict()
    conn, queries = set_up_sql_operations(config_dict)
    update_shutdown_enabled_1(
        conn,
        queries,
    )
    conn.commit()
    conn.close()

    q_server_port = int(config_dict["q_server_port"])

    queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
    queue_server.register("get_satisfy_queue")
    queue_server.register("get_provider_queue")
    queue_server.register("get_want_list_queue")
    queue_server.register("get_remote_monitor_queue")
    queue_server.register("get_publish_queue")
    queue_server.register("get_peer_maint_queue")

    try:
        queue_server.connect()
    except ConnectionRefusedError:
        return
    satisfy_queue = queue_server.get_satisfy_queue()
    provider_queue = queue_server.get_provider_queue()
    want_list_queue = queue_server.get_want_list_queue()
    remote_monitor_queue = queue_server.get_remote_monitor_queue()
    publish_queue = queue_server.get_publish_queue()
    peer_maint_queue = queue_server.get_peer_maint_queue()
    if bool(queue_dict["beacon_enable"]):
        satisfy_queue.put_nowait("0")
    if bool(queue_dict["provider_enable"]):
        provider_queue.put_nowait("1")
    if bool(queue_dict["wantlist_enable"]):
        want_list_queue.put_nowait("2")
    if bool(queue_dict["remote_monitor_enable"]):
        remote_monitor_queue.put_nowait("3")
    if bool(queue_dict["publish_enable"]):
        publish_queue.put_nowait("4")
    if bool(queue_dict["peer_maint_enable"]):
        peer_maint_queue.put_nowait("5")

    return


def shutdown_query(call_stack: str) -> bool:
    from diyims.sqlmodels import Shutdown

    call_stack = call_stack + ":shutdown_query"

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    statement = select(Shutdown)
    with Session(engine) as session:
        results = session.exec(statement)
        try:
            shutdown = results.one()
            enabled = shutdown.enabled
        except NoResultFound:
            pass

    return bool(enabled)


def reset_shutdown(call_stack: str) -> None:
    from diyims.sqlmodels import Shutdown

    call_stack = call_stack + ":reset_shutdown"

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    statement = select(Shutdown)
    with Session(engine) as session:
        results = session.exec(statement)
        try:
            shutdown = results.one()
            if shutdown.enabled == 1:
                shutdown.enabled = 0
                session.add(shutdown)
                session.commit()
        except NoResultFound:
            pass  # TODO: issue not found message

    return


def clean_up(call_stack, roaming):  # TODO: roaming should be set by caller
    call_stack = call_stack + ":clean_up"
    config_dict = get_clean_up_config_dict()

    hours_to_delay = config_dict["hours_to_delay"]
    end_time = datetime.today() - timedelta(hours=int(hours_to_delay))

    clean_up_dict = refresh_clean_up_dict()
    clean_up_dict["insert_DTS"] = end_time.isoformat()
    clean_up_dict["DTS"] = end_time.isoformat()

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    statement = select(Clean_Up).where(Clean_Up.insert_DTS <= end_time.isoformat())
    with Session(engine) as session:
        results = session.exec(statement).all()

        for clean_up in results:
            want_item_file = clean_up.want_item_file
            beacon_CID = clean_up.beacon_CID

            try:
                Path(want_item_file).unlink()
            except FileNotFoundError:
                pass

            param = {
                "arg": beacon_CID,
            }

            response, status_code, response_dict = execute_request(
                url_key="pin_remove",
                param=param,
                call_stack=call_stack,
            )
            # TODO: 700
            session.delete(clean_up)
            session.commit()

        conn, queries = set_up_sql_operations_list(config_dict)
        delete_log_rows_by_date(conn, queries, clean_up_dict)
        conn.commit()

        conn, queries = set_up_sql_operations_list(config_dict)
        delete_want_list_table_rows_by_date(conn, queries, clean_up_dict)
        conn.commit()
        conn.close()

    network_name = get_network_name()

    param = {
        "arg": network_name,
    }

    """
    response, status_code, response_dict = execute_request(
        url_key="pin_add",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        param=param,
    )
    """
    # response, status_code, response_dict = execute_request(
    #    url_key="provide",
    #    logger=logger,
    #    url_dict=url_dict,
    #    config_dict=config_dict,
    #    param=param,
    # )
    # print(status_code)

    return


def test():
    from datetime import datetime
    from time import sleep

    start_DTS = get_DTS()
    start = datetime.fromisoformat(start_DTS)
    sleep(58)
    stop_DTS = get_DTS()
    stop = datetime.fromisoformat(stop_DTS)
    duration = stop - start
    print(duration)


if __name__ == "__main__":
    os.environ["DIYIMS_ROAMING"] = "RoamingDev"
    os.environ["COMPONENT_TEST"] = "1"
    os.environ["QUEUES_ENABLED"] = "0"
    clean_up("__main__", "RoamingDev")
