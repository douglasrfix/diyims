import json
import os
from datetime import datetime
from multiprocessing.managers import BaseManager
from queue import Empty
from time import sleep

from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlmodel import Session, create_engine, or_, select

from diyims.config_utils import get_config_dict
from diyims.general_utils import (
    get_DTS,
    get_network_name,
    get_shutdown_target,
    set_controls,
    set_self,
    shutdown_query,
)
from diyims.logger_utils import add_log
from diyims.path_utils import get_path_dict
from diyims.requests_utils import execute_request
from diyims.sqlmodels import Peer_Table


def provider_capture_main(call_stack: str, peer_type: str) -> None:
    """
    provider_capture_main _summary_

    _extended_summary_

    Arguments:
        call_stack {str} -- _description_
        peer_type {str} -- _description_

    Returns:
        str -- _description_
    """

    call_stack = call_stack + ":provider_capture_main"
    config_dict = get_config_dict('Provider_Capture')
    path_dict = get_path_dict()
    SetControlsReturn = set_controls(call_stack, config_dict)

    SetSelfReturn = set_self()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
    status_code = 200
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
        msg="Provider Capture main startup.",
    )

    target_DT = get_shutdown_target(config_dict)
    max_intervals = int(config_dict["max_intervals"])
    if SetControlsReturn.debug_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Shutdown target {target_DT} or {max_intervals} intervals.",
        )

    network_name = get_network_name()

    if SetControlsReturn.queues_enabled:
        q_server_port = int(config_dict["q_server_port"])
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")

        if peer_type == "PP":
            queue_server.register("get_wantlist_submit_queue")
            queue_server.register("get_provider_queue") # for shutdown purposes
            queue_server.connect()
            wantlist_queue = (
                queue_server.get_wantlist_submit_queue()
            )
            in_bound = queue_server.get_provider_queue()
    else:
        wantlist_queue = None
        in_bound = None

    capture_interval = 0
    total_found = 0
    total_added = 0
    
    current_DT = datetime.now()
    while (
        target_DT > current_DT
        and capture_interval < max_intervals
        and status_code == 200
    ):
        capture_interval += 1

        msg = f"Start of capture interval {capture_interval}"
        if SetControlsReturn.debug_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=msg,
            )
        if shutdown_query(call_stack):
            break
        start_DTS = get_DTS()
        status_code, found, added, duration = (
            capture_providers(
                call_stack,
                peer_type,
                network_name,
                SetControlsReturn.logging_enabled,
                SetControlsReturn.debug_enabled,
                engine,
                SetSelfReturn.self,
            )
        )
        if SetControlsReturn.logging_enabled:
            stop_DTS = get_DTS()
            start = datetime.fromisoformat(start_DTS)
            stop = datetime.fromisoformat(stop_DTS)
            duration = stop - start
            add_log(
                process=call_stack,
                peer_type="status",
                msg=f"capture_providers completed in {duration} seconds with {status_code}.",
            )

        if status_code != 200:
            if SetControlsReturn.debug_enabled:
                add_log(
                    process=call_stack,
                    peer_type="error",
                    msg=f"capture_providers failed with {status_code}.",
                )
            break

        if shutdown_query(call_stack):
            break

        total_found += found
        total_added += added
        
        if SetControlsReturn.queues_enabled:
            wantlist_queue.put_nowait("wake up")


        if SetControlsReturn.logging_enabled:
            msg = f"Interval {capture_interval} complete with find provider duration {duration}."
            add_log(
                process=call_stack,
                peer_type="status",
                msg=msg,
            )

        capture_timeout = int(config_dict["capture_interval_delay"])

        if SetControlsReturn.debug_enabled:
            wantlist_queue.put_nowait("wake up")
            add_log(
                process=call_stack,
                peer_type="status",
                msg=f"Entering interval delay with {capture_timeout}.",
            )

        if SetControlsReturn.queues_enabled:
            try:
                in_bound.get(timeout=capture_timeout)

            except Empty:
                pass
        else:
            sleep(capture_timeout)

        if shutdown_query(call_stack):
            break
        current_DT = datetime.now()

    if SetControlsReturn.metrics_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"{total_found} {peer_type} found, {total_added} added  in {capture_interval} intervals)",
        )
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Provider Capture complete with {status_code}.",
        )


def capture_providers(
    call_stack: str,
    peer_type: str,
    network_name: str,
    logging_enabled: bool,
    debug_enabled: bool,
    engine: str,
    self: str,
) -> tuple[str, str, str, str, str, str, str]:
    """
    capture_providers _summary_

    _extended_summary_

    Arguments:
        call_stack {str} -- _description_
        peer_type {str} -- _description_
        network_name {str} -- _description_
        logging_enabled {bool} -- _description_
        engine {str} -- _description_
        self {str} -- _description_

    Returns:
        tuple[str, str, str, str, str, str, str] -- _description_
    """

    duration = datetime.fromisoformat(get_DTS())
    call_stack = call_stack + ":capture_providers"
    found = 0
    added = 0

    if peer_type == "PP":
        start_DTS = get_DTS()

        if debug_enabled:
            add_log(
                process=call_stack,
                peer_type="error",
                msg="Entering find providers.",
            )

        response, status_code, _response_dict = execute_request(
            url_key="find_providers",
            param={
                "arg": network_name
            },  # the network name is the cid that is provide by every member of the network.
            call_stack=call_stack,
            http_500_ignore=False,
        )
        # by collecting members via a known cid we provide a known peerIDs associated with the cids This is the first step in
        # verifying the hosts are who the claim to be.
        if status_code == 200:
            pass

        elif status_code == 500:  # may not be needed
            if debug_enabled:
                add_log(
                    process=call_stack,
                    peer_type="error",
                    msg=f"IPFS find providers failed {status_code}.",
                )
        else:
            if debug_enabled:
                add_log(
                    process=call_stack,
                    peer_type="error",
                    msg=f"IPFS find providers ended with {status_code}. IPFS seems to not be available.",
                )

            return status_code, found, added, duration

        if status_code == 200:
            stop_DTS = get_DTS()
            start = datetime.fromisoformat(start_DTS)
            stop = datetime.fromisoformat(stop_DTS)
            duration = stop - start

            status_code, found, added = (
                decode_findprovs_structure(
                    call_stack,
                    response,
                    self,
                    logging_enabled,
                    debug_enabled,
                    engine,
                )
            )
            if status_code != 200:  # noqa: SIM102
                if debug_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="error",
                        msg=f"decode findprovs failed with {status_code}.",
                    )

    return status_code, found, added, duration


def decode_findprovs_structure(
    call_stack: str,
    response: str,
    self: str,
    logging_enabled: bool,
    debug_enabled: bool,
    engine: str,
) -> tuple[str, str, str, str, str, str]:
    """
    decode_findprovs_structure _summary_

    _extended_summary_

    Arguments:
        call_stack {str} -- _description_
        response {str} -- _description_
        self {str} -- _description_
        logging_enabled {bool} -- _description_
        engine {str} -- _description_

    Returns:
        tuple[str, str, str, str, str, str] -- _description_
    """

    # Contains early kubo 0.22.0 FINDPROVS speculation.

    # It appears that the output of findprovs  returns an 'ID' value of null for 'Type' 4.
    #'Type' 4 is one of several records(?) in the routing system.

    # The ID can be found in 'Responses'

    # The content of 'Responses' is not JSON. Perhaps list of list? You have to trim the brackets and replace single
    # quotes with double quotes. This was sufficient for my needs but YMMV.

    # This appears to yield the same results as the CLI

    # The routing system has some inertia and retains the node as a provider after the cid is
    # removed i.e. unpinned and a garbage collection has run.

    call_stack = call_stack + ":decode_find_prov"
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"
    engine = create_engine(db_url, echo=False, connect_args={"timeout": 120})
    status_code = 200
    found = 0
    added = 0
    peer_type = "PP"  # legacy artifact from earlier testing

    line_list = []
    for line in response.iter_lines():
        line_list.append(line)  # noqa: PERF402

    for line in line_list:
        decoded_line = line.decode("utf-8")
        line_dict = json.loads(decoded_line)
        if line_dict["Type"] == 4:
            found += 1
            responses_list = line_dict["Responses"]
            for response_b in responses_list:
                list_of_peer_dict = str(response_b).replace(
                    "'", '"'
                )  # needed to use json
                peer_dict = json.loads(str(list_of_peer_dict))
                peer_ID = peer_dict["ID"]
                if self == peer_ID:
                    if logging_enabled:
                        msg = "Local peer was identified as a provider"
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=msg,
                        )

                    break  # break to next provider in responses list

                else:
                    if debug_enabled:
                        msg = f"Primary provider peer {peer_ID} was identified as a provider"
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=msg,
                        )

                    peer_maint_dict = {}
                    peer_maint_dict["peer_ID"] = peer_ID
                    statement = (
                        select(Peer_Table)
                        .where(Peer_Table.peer_ID == peer_ID)
                        .where(
                            or_(
                                Peer_Table.processing_status == "WLR", # existing peer with addresses 
                                Peer_Table.processing_status == "WLRX", #keep
                                Peer_Table.processing_status == "WLW", # Keep
                                Peer_Table.processing_status == "WLWX", # Keep
                                Peer_Table.processing_status == "WLWP", # This would include submitted not started  
                                Peer_Table.processing_status == "WLRP", 
                            )
                        )#TODO: #26 processing_status transition timing
                        #TODO: #29 Is a connection necessary if a wantlist is available?
                    )
                    with Session(engine) as session:
                        results = session.exec(statement)
                        try:
                            current_peer = results.one()
                            status_code = 200
                            existing_peer = True
                        except NoResultFound:
                            status_code = 200
                            existing_peer = False

                    if existing_peer is False:
                        
                        current_peer = Peer_Table(
                            peer_ID=peer_ID,
                            local_update_DTS=get_DTS(),
                            peer_type=peer_type,
                            original_peer_type=peer_type, #TODO: #28 Rationalize peer_type
                            processing_status="WLW",  # new provider without valid address, may be a firewall issue 
                            # WLR #TODO: #12 correct overlapping use of version in peer table.
                            version="4",  # undefined
                            disabled=0,
                        )
                        added += 1

                        with Session(engine) as session:
                            try:
                                session.add(current_peer)
                                session.commit()
                            except IntegrityError:
                                pass

    log_string = f"{found} providers found, {added} added."
    if debug_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=log_string,
        )

    return status_code, found, added


if __name__ == "__main__":

    os.environ["DIYIMS_ROAMING"] = "Roaming"
    os.environ["COMPONENT_TEST"] = "0"
    os.environ["QUEUES_ENABLED"] = "0"
    os.environ["METRICS_ENABLED"] = "0"
    os.environ["LOGGING_ENABLED"] = "0"
    os.environ["DEBUG_ENABLED"] = "0"
    os.environ["CONNECT_ENABLED"] = "0"

    provider_capture_main("__main__", "PP")
