import os
import json
import ipaddress
from queue import Empty
from diyims.requests_utils import execute_request
from datetime import datetime
from time import sleep
from multiprocessing.managers import BaseManager
from multiprocessing import set_start_method, freeze_support
from sqlmodel import create_engine, Session, select, or_
from diyims.general_utils import (
    get_network_name,
    get_shutdown_target,
    get_DTS,
    shutdown_query,
    set_controls,
    set_self,
)
from diyims.logger_utils import add_log
from diyims.config_utils import get_provider_capture_config_dict
from diyims.path_utils import get_path_dict
from diyims.sqlmodels import Peer_Address, Peer_Table
from sqlalchemy.exc import NoResultFound, IntegrityError


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

    if __name__ != "__main__":
        freeze_support()
        try:
            set_start_method("spawn")
        except RuntimeError:
            pass

    call_stack = call_stack + ":provider_capture_main"
    config_dict = get_provider_capture_config_dict()
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
            queue_server.register("get_provider_queue")
            queue_server.connect()
            out_bound = (
                queue_server.get_wantlist_submit_queue()
            )  # TODO: process queue???
            in_bound = queue_server.get_provider_queue()
    else:
        out_bound = None
        in_bound = None

    capture_interval = 0
    total_found = 0
    total_added = 0
    total_promoted = 0
    total_released = 0

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
        status_code, found, added, promoted, duration, released, modified = (
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
        total_promoted += promoted
        total_released += released

        if released > 0:
            if SetControlsReturn.queues_enabled:
                out_bound.put_nowait("wake up")
                if SetControlsReturn.debug_enabled:  # no sense in sending a message when a peer is release since they are single threaded by wantlist
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=f"Sent wakeup with {released} released.",
                    )

        if SetControlsReturn.logging_enabled:
            msg = f"Interval {capture_interval} complete with find provider duration {duration} and {released} released."
            add_log(
                process=call_stack,
                peer_type="status",
                msg=msg,
            )

        capture_timeout = int(config_dict["capture_interval_delay"])

        if SetControlsReturn.debug_enabled:
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
            msg=f"{total_found} {peer_type} found, {total_promoted} promoted, {total_added} added and {total_released} released  in {capture_interval} intervals)",
        )
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Provider Capture complete with {status_code}.",
        )
    return


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
    promoted = 0
    released = 0
    modified = 0

    if peer_type == "PP":
        start_DTS = get_DTS()

        if debug_enabled:
            add_log(
                process=call_stack,
                peer_type="error",
                msg="Entering find providers.",
            )

        response, status_code, response_dict = execute_request(
            url_key="find_providers",
            param={"arg": network_name},
            call_stack=call_stack,
            http_500_ignore=False,
        )

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

            return status_code, found, added, promoted, duration, released, modified

        if status_code == 200:
            stop_DTS = get_DTS()
            start = datetime.fromisoformat(start_DTS)
            stop = datetime.fromisoformat(stop_DTS)
            duration = stop - start

            status_code, found, added, promoted, released, modified = (
                decode_findprovs_structure(
                    call_stack,
                    response,
                    self,
                    logging_enabled,
                    debug_enabled,
                    engine,
                )
            )
            if status_code != 200:
                if debug_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="error",
                        msg=f"decode findprovs failed with {status_code}.",
                    )

    return status_code, found, added, promoted, duration, released, modified


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
    promoted = 0
    modified = 0
    released = 0
    peer_type = "PP"  # NOTE: artifact from earlier testing

    line_list = []
    for line in response.iter_lines():
        line_list.append(line)

    for line in line_list:
        address_available = False
        decoded_line = line.decode("utf-8")
        line_dict = json.loads(decoded_line)
        if line_dict["Type"] == 4:
            found += 1
            responses_list = line_dict["Responses"]
            for response in responses_list:
                list_of_peer_dict = str(response).replace(
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

                    add_required = False
                    # peer_waiting = False
                    existing_peer = False
                    address_list = peer_dict["Addrs"]

                    peer_maint_dict = {}
                    peer_maint_dict["peer_ID"] = peer_ID
                    statement = (
                        select(Peer_Table)
                        .where(Peer_Table.peer_ID == peer_ID)
                        .where(
                            or_(
                                Peer_Table.processing_status == "WLR",
                                Peer_Table.processing_status == "WLRX",
                                Peer_Table.processing_status == "WLW",
                                Peer_Table.processing_status == "WLWX",
                                Peer_Table.processing_status == "WLWF",
                            )
                        )
                    )
                    with Session(engine) as session:
                        results = session.exec(statement)
                        try:
                            current_peer = results.one()
                            # peer_dict["processing_status"] = current_peer.processing_status
                            status_code = 200
                            existing_peer = True
                        except NoResultFound:
                            status_code = 200
                            existing_peer = False

                    if existing_peer:
                        if (
                            current_peer.processing_status == "WLW"
                            or current_peer.processing_status == "WLWP"
                            or current_peer.processing_status == "WLWX"
                        ):  # TODO: This can be WLWP or WLWX oor WLW?
                            status_code, address_available = capture_provider_addresses(
                                call_stack,
                                address_list,
                                peer_ID,
                                logging_enabled,
                                debug_enabled,
                                engine,
                            )

                            status_code = 200  # ignore any address capture failure
                        else:
                            pass  # in wlr or something else status and won't look for addresses
                    else:  # new provider found
                        status_code, address_available = capture_provider_addresses(
                            call_stack,
                            address_list,
                            peer_ID,
                            logging_enabled,
                            debug_enabled,
                            engine,
                        )
                        status_code = 200  # tolerate absence of peer entry
                        if debug_enabled:
                            msg = f"New provider found for {peer_ID}"
                            add_log(
                                process=call_stack,
                                peer_type="status",
                                msg=msg,
                            )
                        if address_available:
                            current_peer = Peer_Table(
                                peer_ID=peer_ID,
                                local_update_DTS=get_DTS(),
                                peer_type=peer_type,
                                original_peer_type=peer_type,
                                processing_status="WLR",  # new provider with valid address
                                version="1",  # WLR
                                disabled=0,
                            )
                            added += 1
                            released += 1
                            add_required = True
                        else:
                            current_peer = Peer_Table(
                                peer_ID=peer_ID,
                                local_update_DTS=get_DTS(),
                                peer_type=peer_type,
                                original_peer_type=peer_type,
                                processing_status="WLW",  # new provider without valid address, may be a firewall issue
                                version="4",  # undefined
                                disabled=0,
                            )
                            added += 1
                            add_required = True

                    if add_required:  # TODO this should trigger a comm request add.
                        with Session(engine) as session:
                            try:
                                session.add(current_peer)
                                session.commit()
                            except IntegrityError:
                                pass

                if address_available:
                    break  # break to next provider in responses list

    log_string = f"{found} providers found, {added} added, {released} released and {promoted} promoted."
    if debug_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=log_string,
        )

    return status_code, found, added, promoted, released, modified


def capture_provider_addresses(
    call_stack: str,
    address_list: list,
    peer_ID: str,
    logging_enabled: bool,
    debug_enabled: bool,
    engine: str,
) -> tuple[str, bool, int]:
    """
    capture_provider_addresses _summary_

    _extended_summary_

    Arguments:
        call_stack {str} -- _description_
        address_list {list} -- _description_
        peer_ID {str} -- _description_
        logging_enabled {bool} -- _description_
        engine {str} -- _description_

    Returns:
        tuple[str, bool, int] -- _description_
    """

    call_stack = call_stack + ":capture_provider_addresses"
    address_available = False
    address_source = "PP"

    start_DTS = get_DTS()
    capture_addresses(  # provider is source FP
        call_stack,
        address_list,
        peer_ID,
        address_source,
        engine,
    )

    param = {"arg": peer_ID}
    response, status_code, response_dict = execute_request(  # gather peer addresses
        url_key="id",
        param=param,
        call_stack=call_stack,
        http_500_ignore=False,
        connect_retries=1,
    )
    if status_code == 200:
        address_source = "ID"
        peer_dict = json.loads(response.text)
        address_list = peer_dict["Addresses"]
        capture_addresses(
            call_stack,  # id is source ID
            address_list,
            peer_ID,
            address_source,
            engine,
        )

    else:
        msg = f"IPFS id request failed with {status_code}. Peer {peer_ID} is most likely unreachable."
        if debug_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=msg,
            )

    statement = select(Peer_Address).where(
        Peer_Address.peer_ID == peer_ID,
        Peer_Address.available == 1,
    )
    with Session(engine) as session:
        results = session.exec(statement)
        if results.first() is None:
            address_available = False
        else:
            address_available = True

    stop_DTS = get_DTS()
    start = datetime.fromisoformat(start_DTS)
    stop = datetime.fromisoformat(stop_DTS)
    duration = stop - start

    msg = f"Capture provider addresses in {duration} seconds for {peer_ID}."
    if debug_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=msg,
        )

    return status_code, address_available


def capture_addresses(
    call_stack: str,
    address_list: list,
    peer_ID: str,
    address_source: str,
    engine: str,
) -> None:
    """
    capture_addresses _summary_

    _extended_summary_

    Arguments:
        call_stack {str} -- _description_
        address_list {list} -- _description_
        peer_ID {str} -- _description_
        address_source {str} -- _description_
        engine {str} -- _description_
    """

    call_stack = call_stack + ":capture_addresses"
    for address in address_list:
        address_string = address
        address_ignored = False
        ignored_reason = ""
        address_global = False
        multiaddress = ""
        address_type = ""
        protocol_valid = False
        available = False
        port_valid = False
        multiaddress_valid = False

        index = address.lower().find("/p2p-circuit")
        if index != -1:
            address_ignored = True
            ignored_reason = "/p2p-circuit"

        if not address_ignored:
            index = address.lower().find("/webtransport")
            if index != -1:
                address_ignored = True
                ignored_reason = "/webtransport"

        if not address_ignored:
            index = address.lower().find("/webrtc-direct")
            if index != -1:
                address_ignored = True
                ignored_reason = "/webrtc-direct"

        if not address_ignored:
            index = address.lower().find("/web")
            if index != -1:
                address_ignored = True
                ignored_reason = "/web"

        if not address_ignored:
            index = address.lower().find("/dns4")
            if index != -1:
                address_ignored = True
                ignored_reason = "/dns4"

        if not address_ignored:
            index = address.lower().find("/dns")
            if index != -1:
                address_ignored = True
                ignored_reason = "/dns"

        if not address_ignored:
            index = address.lower().find("/tls")
            if index != -1:
                address_ignored = True
                ignored_reason = "/tls"

        if not address_ignored:
            ip_version = address[:5]
            index = address.lower().find("/", 5)
            ip_index = index
            ip_string = address[5:index]
            if ip_version == "/ip4/":
                address_type = "4"
                if ipaddress.IPv4Address(ip_string).is_global:
                    address_global = True

            else:
                address_type = "6"
                if ipaddress.IPv6Address(ip_string).is_global:
                    address_global = True

            index = address.lower().find("/tcp/", ip_index)
            if index != -1:
                protocol_valid = True
                ip_index = index + 5

            if not protocol_valid:
                index = address.lower().find("/udp/", ip_index)
                if index != -1:
                    protocol_valid = True
                    ip_index = index + 5

            if protocol_valid:
                index = address.lower().find("/", ip_index)  # naked port
                if index == -1:
                    port_start = ip_index
                    port = address[port_start:]
                    if port.isnumeric():
                        port_valid = True
                        multiaddress = address + "/p2p/" + peer_ID
                        multiaddress_valid = True

                if not port_valid:
                    port_start = ip_index
                    port_end = index - 1
                    port = address[port_start:port_end]
                    if port.isnumeric():
                        port_valid = True
                        ip_index = index

                if port_valid:
                    index = address.lower().find("/quic-v1/", ip_index)
                    if index == -1:
                        index = address.lower().find("/quic-v1", ip_index)
                        if index != -1:
                            multiaddress = address + "/p2p/" + peer_ID
                            multiaddress_valid = True
                    else:
                        index = address.lower().find("/p2p/", ip_index)
                        if index != -1:
                            ip_index = index + 5
                            index = address.lower().find("/", ip_index)
                            if index == -1:
                                multiaddress = address
                                multiaddress_valid = True

                    if not multiaddress_valid:
                        index = address.lower().find("/p2p/", ip_index)
                        if index != -1:
                            ip_index = index + 5
                            index = address.lower().find("/", ip_index)
                            if index == -1:
                                multiaddress = address
                                multiaddress_valid = True

        if not address_ignored and multiaddress_valid and address_global:
            available = True

        insert_DTS = get_DTS()
        create_peer_address(
            peer_ID,
            multiaddress,
            insert_DTS,
            address_ignored,
            ignored_reason,
            address_string,
            address_type,
            address_source,
            address_global,
            available,
            engine,
        )

    return


def create_peer_address(
    peer_ID: str,
    multiaddress: str,
    insert_DTS: str,
    address_ignored: bool,
    ignored_reason: str,
    address_string: str,
    address_type: str,
    address_source: str,
    address_global: bool,
    available: bool,
    engine: str,
) -> None:
    """
    create_peer_address _summary_

    _extended_summary_

    Arguments:
        peer_ID {str} -- _description_
        multiaddress {str} -- _description_
        insert_DTS {str} -- _description_
        address_ignored {bool} -- _description_
        ignored_reason {str} -- _description_
        address_string {str} -- _description_
        address_type {str} -- _description_
        address_source {str} -- _description_
        address_global {bool} -- _description_
        available {bool} -- _description_
        engine {str} -- _description_
    """

    address_row = Peer_Address(
        peer_ID=peer_ID,
        multiaddress=multiaddress,
        insert_DTS=insert_DTS,
        address_ignored=address_ignored,
        ignored_reason=ignored_reason,
        address_string=address_string,
        address_type=address_type,
        address_source=address_source,
        address_global=address_global,
        available=available,
    )
    statement = select(Peer_Address).where(
        Peer_Address.peer_ID == peer_ID, Peer_Address.address_string == address_string
    )
    with Session(engine) as session:
        results = session.exec(statement).first()
        if results is None:
            session.add(address_row)
            session.commit()
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

    provider_capture_main("__main__", "PP")
