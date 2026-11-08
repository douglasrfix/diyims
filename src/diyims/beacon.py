"""
Beacon creates a unique CID that is used to generate an item in a nodes wantlist.           
"""

# "Beacon creates a temporary wantlist item to allow a remote peer the ability
# to discover and access the local peer's communication and verification information.
# """

import json
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep

from sqlmodel import Session, col, create_engine, select

from diyims.config_utils import get_config_dict
from diyims.error_classes import CommunicationsError
from diyims.general_utils import (
    get_DTS,
    get_shutdown_target,
    set_controls,
    set_self,
    shutdown_query,
)
from diyims.logger_utils import add_log
from diyims.path_utils import get_path_dict
from diyims.requests_utils import execute_request
from diyims.sqlmodels import Beacon, Header_Table


def beacon_main(call_stack: str) -> None:
    """
    beacon_main _summary_

    _extended_summary_

    Arguments:
        call_stack {str} -- _description_

    Returns:
        str -- _description_
    """

    call_stack = call_stack + ":beacon_main"
    config_dict = get_config_dict('Beacon')
    path_dict = get_path_dict()
    SetControlsReturn = set_controls(call_stack, config_dict)
    SetSelfReturn = set_self()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
        
    clean_up(call_stack, engine, path_dict, config_dict, 'startup')
    
    status_code = 200
    # find the first pl entry in the header table.  This would be from the initial set up of the application
    # the entry contains the cid of the peer record of the local provider
    # which is the root header for that host
    statement = (
        select(Header_Table)
        .where(Header_Table.peer_ID == SetSelfReturn.self)
        .order_by(col(Header_Table.insert_DTS).asc())
    )
    with Session(engine) as session:
        results = session.exec(statement)
        header_row = results.first()

    peer_row_CID = header_row.object_CID

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
        msg="Beacon main startup.",
    )

    interval = 0
    target_DT = get_shutdown_target(config_dict)
    current_DT = datetime.now()

    max_intervals = int(config_dict["max_intervals"])
    if SetControlsReturn.debug_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Shutdown target {target_DT} or {max_intervals} intervals.",
        )

    while target_DT > current_DT and interval < max_intervals and status_code == 200:
        if shutdown_query(call_stack):
            break
        
        clean_up(call_stack, engine, path_dict, config_dict, 'by_age')
        
        for _ in range(int(config_dict["number_of_periods"])):
            if shutdown_query(call_stack):
                break

            status_code, beacon_CID = create_beacon_CID(
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
                
                status_code = flash_beacon(
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
                    break

                else:
                    if shutdown_query(call_stack):
                        break
                     
                    satisfy_beacon(call_stack, 
                                    SetControlsReturn.logging_enabled,
                                    SetControlsReturn.debug_enabled,
                                    path_dict,
                                    engine,)

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


def create_beacon_CID(
    call_stack: str,
    logging_enabled: bool,
    debug_enabled: bool,
    config_dict: str,
    peer_row_CID: str,
    path_dict: dict,
    engine: str,
    ) -> tuple[
    str,
    str,
    ]:
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
    )
    satisfy_timedelta = timedelta(seconds=satisfy_wait_seconds)

    want_item_dict = {}
    want_item_dict["peer_row_CID"] = (
        peer_row_CID  # this is the first entry in the header table and points to a peer image
    )
    want_item_dict["DTS"] = get_DTS()  # the dts creates a unique cid
    want_item_file = str(path_dict["want_item_file"])  

    #with open(want_item_file, "w", encoding="utf-8", newline="\n") as write_file: #TODO: #35 standardize encoding
    with open(want_item_file, "w", newline="\n") as write_file:
        json.dump(want_item_dict, write_file, indent=4)

    with open(want_item_file, "rb") as f:
        file = {"file": f}
        param = {"only-hash": "true", "pin": "false", "cid-version": 1} # this provides a cid that can be duplicated by satisfy when the time comes
        _response, status_code, response_dict = execute_request(
            url_key="add",
            param=param,
            file=file,
            call_stack=call_stack,
            http_500_ignore=False,
        )


    if status_code == 200:
        beacon_CID = response_dict["Hash"]
        want_item_dict_str = json.dumps(want_item_dict, indent=4)
        insert_DTS = get_DTS()
        insert_datetime = datetime.fromisoformat(insert_DTS)
        satisfy_target_DTS = insert_datetime + satisfy_timedelta
        beacon_entry = Beacon(
            insert_DTS=insert_DTS,
            satisfy_target_DTS=satisfy_target_DTS,
            status="new",
            want_item_dict_str=want_item_dict_str,
            beacon_CID=beacon_CID,
        )

        with Session(engine) as session:
            session.add(beacon_entry)
            session.commit()
        if debug_enabled:
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
    return status_code, beacon_CID


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
    if debug_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Flash on.",
        )
    
    beacon_secs = str(config_dict["beacon_length_seconds"])
    _response, status_code, _response_dict = execute_request(
        url_key="get",
        param={
            "arg": beacon_CID,
            "timeout": beacon_secs + "s" # this implements the beacon length in seconds
        },
        timeout=(
            3.05,
            310,
        ),
        call_stack=call_stack,
    )
    if status_code != 200:
        if status_code == 500: # this catches the error from the time out which is expected 
            status_code = 200
        if debug_enabled:
            add_log(
                process=call_stack,
                peer_type="Error",
                msg=f"Beacon flash panic, ipfs get failed with {status_code} for {beacon_CID}.",
            )

    if debug_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg="Flash off.",
        )

    return status_code

def clean_up(
    call_stack: str, engine, path_dict, config_dict, type,
):
    call_stack = call_stack + ":clean_up"
    
    hours_to_delay = config_dict["hours_to_delay"]
    insert_DTS = get_DTS()
    end_time = datetime.fromisoformat(insert_DTS) - timedelta(hours=int(hours_to_delay))

    if type == 'by_age':
        call_stack = call_stack + ":by_age"
        statement = select(Beacon).where(Beacon.insert_DTS <= end_time.isoformat())
    elif type == 'startup':
        call_stack = call_stack + ":start"
        statement = select(Beacon).where(Beacon.status == "new")

    with Session(engine) as session:
        results = session.exec(statement).all()

        for beacon in results:
            beacon_CID = beacon.beacon_CID
            param = {
                "arg": beacon_CID,
            }
            try:
                _response, _status_code, _response_dict = execute_request(
                    url_key="pin_remove",
                    param=param,
                    call_stack=call_stack,
                    http_500_ignore=True,
                )
            except CommunicationsError as e:
                
                add_log(
                    process=call_stack,
                    peer_type="info",
                    msg=f"pin remove failed with {e} for {beacon_CID}", #TODO: #34 Separate Comm Logging
                )
    
            session.delete(beacon)
            session.commit()

            if type == 'startup':

                try:
                    want_item_file = str(path_dict["want_item_file"])
                    Path(want_item_file).unlink()
                except FileNotFoundError:
                    pass

def satisfy_beacon(
    call_stack: str,
    logging_enabled: bool,
    debug_enabled: bool,
    path_dict: dict,
    engine,
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

    
    statement = select(Beacon).where(Beacon.status == "new")
    with Session(engine) as session:
        results = session.exec(statement)
        beacon = results.one()

        # this makes the item on the wantlist available for retrieval.
        # the content points to the peer table entry for this peer.  
        want_item_dict = json.loads(beacon.want_item_dict_str)
        want_item_file = str(path_dict["want_item_file"])
        # This file originated in the create beacon function and its name is fixed by configuration
        with open(want_item_file, "w", encoding="utf-8", newline="\n") as write_file:
            json.dump(want_item_dict, write_file, indent=4)

        beacon.status = "old"
        
    with Session(engine) as session:
        session.add(beacon)
        session.commit()

    with open(want_item_file, "rb") as f:
        file = {"file": f}

        param = {
            "only-hash": "false",
            "pin": "true",
            "cid-version": 1,
            "pin-name": "diyims",
        }
        _response, status_code, _response_dict = execute_request(
            url_key="add",
            param=param,
            file=file,
            call_stack=call_stack,
            http_500_ignore=False,
        )

    try:
        Path(want_item_file).unlink()
    except FileNotFoundError:
        pass

    if status_code != 200:  # noqa: SIM102
        if debug_enabled:
            add_log(
                process=call_stack,
                peer_type="error",
                msg=f"Satisfy ipfs add failed with {status_code}.",
            )

    if debug_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Satisfy {want_item_file}",
        )
    return status_code



if __name__ == "__main__":
    import os
    
    os.environ["DIYIMS_ROAMING"] = "Roaming"
    os.environ["COMPONENT_TEST"] = "0"
    os.environ["QUEUES_ENABLED"] = "0"
    os.environ["LOGGING_ENABLED"] = "1"

    status_code = beacon_main(
        "__main__",
    )
