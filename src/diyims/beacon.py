"""
Beacon creates a unique CID that is used to generate a item in a nodes wantlist.
"""

# "Beacon creates a temporary wantlist item to allow a remote peer the ability
# to discover and access the local peer's communication and verification information.
# """
from time import sleep
from datetime import datetime, timedelta
from multiprocessing import Process, set_start_method, freeze_support

# from diyims.class_imports import SetSelfReturn
from diyims.requests_utils import execute_request
from diyims.config_utils import get_beacon_config_dict
from diyims.logger_utils import add_log
from diyims.general_utils import (
    get_shutdown_target,
    shutdown_query,
    set_controls,
    set_self,
)
from diyims.satisfy import satisfy_main
import json
from sqlalchemy.exc import NoResultFound
from sqlmodel import create_engine, Session, select, col
from diyims.sqlmodels import Clean_Up, Header_Table
from diyims.path_utils import get_path_dict, get_unique_file
from diyims.general_utils import get_DTS


def beacon_main(call_stack: str) -> None:
    """
    beacon_main _summary_

    _extended_summary_

    Arguments:
        call_stack {str} -- _description_

    Returns:
        str -- _description_
    """

    if __name__ != "__main__":
        freeze_support()
        try:
            set_start_method("spawn")
        except RuntimeError:
            pass

    call_stack = call_stack + ":beacon_main"
    config_dict = get_beacon_config_dict()
    path_dict = get_path_dict()
    SetControlsReturn = set_controls(call_stack, config_dict)
    SetSelfReturn = set_self()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
    status_code = 200
    statement = (
        select(Header_Table)
        .where(Header_Table.peer_ID == SetSelfReturn.self)
        .where(Header_Table.object_type == "local_peer_entry")
        .order_by(col(Header_Table.insert_DTS).asc())
    )
    with Session(engine) as session:
        results = session.exec(statement)
        header_row = results.first()
    peer_row_CID = header_row.object_CID

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
        msg="Beacon main startup.",
    )

    interval = 0
    target_DT = get_shutdown_target(config_dict)
    current_DT = datetime.now()

    max_intervals = int(config_dict["max_intervals"])
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Shutdown target {target_DT} or {max_intervals} intervals.",
        )

    while target_DT > current_DT and interval < max_intervals and status_code == 200:
        if shutdown_query(call_stack):  # exit while loop
            break
        for _ in range(int(config_dict["number_of_periods"])):
            if shutdown_query(call_stack):
                break  # exit for loop

            status_code, beacon_CID, want_item_file = create_beacon_CID(
                call_stack,
                SetControlsReturn.logging_enabled,
                SetControlsReturn.debug_enabled,
                config_dict,
                peer_row_CID,
                path_dict,
                engine,
            )

            if shutdown_query(call_stack):
                break

            if status_code == 200:
                satisfy_main_process = Process(target=satisfy_main, args=(call_stack,))
                satisfy_main_process.start()

                status_code = flash_beacon(  # flash waits on satisfy_main
                    call_stack,
                    SetControlsReturn.logging_enabled,
                    SetControlsReturn.debug_enabled,
                    SetControlsReturn.component_test,
                    config_dict,
                    beacon_CID,
                )

                if status_code != 200:
                    if SetControlsReturn.debug_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="error",
                            msg=f"Beacon Panic from flash beacon with {status_code}.",
                        )
                    break  # beacon will end and cleanup satisfy

                satisfy_main_process.join()  # wait for satisfy to end

            else:
                if SetControlsReturn.debug_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="Error",
                        msg=f"Beacon Panic from create beacon with {status_code}.",
                    )
                break

        if status_code == 200:
            interval += 1
            current_DT = datetime.now()
        else:
            break

    add_log(
        process=call_stack,
        peer_type="status",
        msg=f"Beacon shutdown with {status_code}.",
    )
    return


def create_beacon_CID(
    call_stack: str,
    logging_enabled: bool,
    debug_enabled: bool,
    config_dict: str,
    peer_row_CID: str,
    path_dict: dict,
    engine: str,
) -> tuple[str, str, str]:
    """
    create_beacon_CID _summary_

    _extended_summary_

    Arguments:
        call_stack {str} -- _description_
        logging_enabled {bool} -- _description_
        debug_enabled {bool} -- _description_
        config_dict {str} -- _description_
        peer_row_CID {str} -- _description_
        path_dict {dict} -- _description_
        engine {str} -- _description_

    Returns:
        tuple[str, str, str] -- _description_
    """

    call_stack = call_stack + ":create_beacon_CID"

    satisfy_wait_seconds = int(
        config_dict["beacon_length_seconds"]
    )  # This will be used to define the duration of the want_item on the want list.
    satisfy_timedelta = timedelta(seconds=satisfy_wait_seconds)

    want_item_dict = {}
    want_item_dict["peer_row_CID"] = peer_row_CID
    want_item_dict["DTS"] = get_DTS()

    want_item_path = path_dict["want_item_path"]
    proto_item_file = path_dict["want_item_file"]
    want_item_file_path = get_unique_file(want_item_path, proto_item_file)
    want_item_file = str(want_item_file_path)

    with open(want_item_file_path, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(want_item_dict, write_file, indent=4)

    f = open(want_item_file_path, "rb")
    file = {"file": f}
    param = {"only-hash": "true", "pin": "false", "cid-version": 1}
    response, status_code, response_dict = execute_request(
        url_key="add",
        param=param,
        file=file,
        call_stack=call_stack,
        http_500_ignore=False,
    )
    f.close()

    if status_code == 200:
        beacon_CID = response_dict["Hash"]

        statement = select(Clean_Up).where(Clean_Up.status == "new")
        with Session(engine) as session:
            results = session.exec(statement)
            try:
                clean_up = results.one()
                one_available = True
                clean_up.status = "old"
            except NoResultFound:
                one_available = False

        insert_DTS = get_DTS()
        insert_datetime = datetime.fromisoformat(insert_DTS)
        satisfy_target_DTS = insert_datetime + satisfy_timedelta
        clean_up_entry = Clean_Up(
            insert_DTS=insert_DTS,
            satisfy_target_DTS=satisfy_target_DTS,
            status="new",
            want_item_file=want_item_file,
            beacon_CID=beacon_CID,
        )

        with Session(engine) as session:
            if one_available:
                session.add(clean_up)
            session.add(clean_up_entry)
            session.commit()
        if logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=f"Create {beacon_CID}",
            )
    else:
        want_item_file = None
        beacon_CID = None
        if debug_enabled:
            add_log(
                process=call_stack,
                peer_type="error",
                msg=f"Beacon not created ipfs add failed with {status_code} for peer table row CID {peer_row_CID}",
            )
    return status_code, beacon_CID, want_item_file


def flash_beacon(
    call_stack: str,
    logging_enabled: bool,
    debug_enabled: bool,
    component_test: bool,
    config_dict: str,
    beacon_CID: str,
) -> str:
    """
    flash_beacon _summary_

    _extended_summary_

    Arguments:
        call_stack {str} -- _description_
        logging_enabled {bool} -- _description_
        debug_enabled {bool} -- _description_
        component_test {bool} -- _description_
        config_dict {str} -- _description_
        beacon_CID {str} -- _description_

    Returns:
        str -- _description_
    """

    call_stack = call_stack + ":flash_beacon"
    status_code = 200
    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Flash on.",
        )

    if component_test:
        sleep(int(config_dict["beacon_length_seconds"]))
        status_code = 200

    else:
        response, status_code, response_dict = execute_request(  # wait for satisfy
            url_key="get",
            param={
                "arg": beacon_CID,
            },
            timeout=(
                3.05,
                310,
            ),  # control time out to avoid retries should this be calculated???????
            call_stack=call_stack,
        )
        if status_code != 200:
            if debug_enabled:
                add_log(
                    process=call_stack,
                    peer_type="Error",
                    msg=f"Beacon flash panic, ipfs get failed with {status_code} for {beacon_CID}.",
                )

    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Flash off.",
        )

    return status_code


if __name__ == "__main__":
    from multiprocessing import set_start_method, freeze_support
    import os

    freeze_support()
    set_start_method("spawn")

    os.environ["DIYIMS_ROAMING"] = "RoamingDev"
    os.environ["COMPONENT_TEST"] = "1"
    os.environ["QUEUES_ENABLED"] = "0"

    status_code = beacon_main(
        "__main__",
    )
