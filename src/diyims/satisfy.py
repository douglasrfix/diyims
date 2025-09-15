from multiprocessing.managers import BaseManager
from diyims.config_utils import get_beacon_config_dict
from diyims.logger_utils import add_log
from datetime import datetime
from time import sleep
from queue import Empty
from sqlmodel import create_engine, Session, select
from diyims.sqlmodels import Clean_Up
from diyims.requests_utils import execute_request
from diyims.path_utils import get_path_dict
from diyims.general_utils import get_DTS, set_controls


def satisfy_main(call_stack: str) -> None:
    """
    satisfy_main _summary_

    _extended_summary_

    Arguments:
        call_stack {str} -- _description_

    Returns:
        str -- _description_
    """

    call_stack = call_stack + ":satisfy_main"
    config_dict = get_beacon_config_dict()
    path_dict = get_path_dict()
    SetControlsReturn = set_controls(call_stack, config_dict)

    if SetControlsReturn.queues_enabled:
        q_server_port = int(config_dict["q_server_port"])
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
        queue_server.register("get_satisfy_queue")
        queue_server.connect()
        in_bound_wait = queue_server.get_satisfy_queue()
    else:
        in_bound_wait = None

    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Satisfy start",
        )

    status_code = satisfy_beacon(
        call_stack,
        SetControlsReturn.logging_enabled,
        SetControlsReturn.debug_enabled,
        SetControlsReturn.queues_enabled,
        in_bound_wait,
        path_dict,
    )

    if status_code != 200:
        if SetControlsReturn.logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=f"Satisfy Panic satisfy beacon failed with {status_code}.",
            )

    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Satisfy complete.",
        )

    return


def satisfy_beacon(
    call_stack: str,
    logging_enabled: bool,
    debug_enabled: bool,
    queues_enabled: bool,
    in_bound_wait: str,
    path_dict: dict,
) -> str:
    """
    satisfy_beacon _summary_

    _extended_summary_

    Arguments:
        call_stack {str} -- _description_
        logging_enabled {bool} -- _description_
        debug_enabled {bool} -- _description_
        config_dict {dict} -- _description_
        queues_enabled {bool} -- _description_
        in_bound_wait {str} -- _description_
        path_dict {dict} -- _description_

    Returns:
        str -- _description_
    """
    call_stack = call_stack + ":satisfy_beacon"
    status_code = 200

    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    statement = select(Clean_Up).where(Clean_Up.status == "new")
    with Session(engine) as session:
        results = session.exec(statement)
        clean_up = results.one()

        clean_up.status = "old"
        clean_up_target = clean_up.satisfy_target_DTS
        want_item_file = clean_up.want_item_file

    with Session(engine) as session:
        session.add(clean_up)
        session.commit()

    target = datetime.fromisoformat(clean_up_target)
    now_DTS = datetime.fromisoformat(get_DTS())

    wait_seconds = (target - now_DTS).total_seconds()

    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Satisfy beacon with {wait_seconds} second wait.",
        )

    if queues_enabled:
        try:
            in_bound_wait.get(timeout=int(wait_seconds))
        except Empty:
            pass
    else:
        sleep(int(wait_seconds))

    f = open(want_item_file, "rb")
    file = {"file": f}

    param = {"only-hash": "false", "pin": "true", "cid-version": 1}
    response, status_code, response_dict = execute_request(
        url_key="add",
        param=param,
        file=file,
        call_stack=call_stack,
        http_500_ignore=False,
    )
    f.close()

    if status_code != 200:
        if debug_enabled:
            add_log(
                process=call_stack,
                peer_type="error",
                msg=f"Satisfy ipfs add failed with {status_code}.",
            )

    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Satisfy {want_item_file}",
        )
    return status_code
