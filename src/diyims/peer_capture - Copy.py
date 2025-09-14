"""
Contains early kubo 0.22.0 FINDPROVS speculation.

It appears that the output of findprovs  returns an 'ID' value of null for 'Type' 4.
'Type' 4 is one of several records(?) in the routing system.

The ID can be found in 'Responses'

The content of 'Responses' is not JSON. Perhaps list of list? You have to trim the brackets and replace single
quotes with double quotes. This was sufficient for my needs but YMMV.

This appears to yield the same results as the CLI

The routing system has some inertia and retains the node as a provider after the cid is
removed i.e. unpinned and a garbage collection has run.

"""

import os
import json
import ipaddress
import psutil
from queue import Empty
from diyims.requests_utils import execute_request
from datetime import datetime
from sqlite3 import IntegrityError
from time import sleep
from multiprocessing.managers import BaseManager
from multiprocessing import set_start_method, freeze_support
from sqlmodel import create_engine, Session, select

# from diyims.ipfs_utils import get_url_dict
from diyims.database_utils import (
    insert_peer_row,
    refresh_peer_row_from_template,
    # select_peer_table_entry_by_key,
    set_up_sql_operations,
    refresh_log_dict,
    # insert_log_row,
    # update_peer_table_status_WLW_to_WLR,
    # update_peer_table_version,
    update_peer_table_peer_type_BP_to_PP,
    update_peer_table_peer_type_SP_to_PP,
)
from diyims.general_utils import (
    get_network_name,
    get_shutdown_target,
    get_DTS,
    shutdown_query,
)
from diyims.logger_utils import add_log
from diyims.config_utils import get_provider_capture_config_dict
from diyims.path_utils import get_path_dict
from diyims.sqlmodels import Peer_Address, Peer_Table
from sqlalchemy.exc import NoResultFound

#  psutil.BELOW_NORMAL_PRIORITY_CLASS,
#  psutil.NORMAL_PRIORITY_CLASS,
#  psutil.ABOVE_NORMAL_PRIORITY_CLASS,
#  psutil.HIGH_PRIORITY_CLASS,
#  psutil.REALTIME_PRIORITY_CLASS

# class Peer_Address(SQLModel, table=True):
#    peer_ID: str = Field(primary_key=True)
#    multiaddress: str = Field(primary_key=True)
#    insert_timestamp: str | None = None


def provider_capture_main(call_stack, peer_type):
    if __name__ != "__main__":
        freeze_support()
        try:
            set_start_method("spawn")
        except RuntimeError:
            pass
    call_stack = call_stack + ":provider_capture_main"
    config_dict = get_provider_capture_config_dict()
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
    debug_enabled = bool(int(config_dict["debug_enabled"]))

    if logging_enabled:
        if queues_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Queues enabled",
            )
        if debug_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Debug enabled",
            )
        if component_test:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Component test enabled",
            )

    wait_before_startup = int(config_dict["wait_before_startup"])
    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Waiting for {wait_before_startup} seconds before startup.",
        )
    sleep(wait_before_startup)

    add_log(
        process=call_stack,
        peer_type="status",
        msg="Capture Provider startup.",
    )

    # if peer_type == "PP":
    # p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # TODO: put in config

    # logger.info("Provider Capture startup.")
    # elif peer_type == "BP":
    # p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # TODO: put in config

    # logger.info("Startup of Bitswap Capture.")
    # elif peer_type == "SP":
    # p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # TODO: put in config

    # logger.info("Startup of Swarm Capture.")

    target_DT = get_shutdown_target(config_dict)
    max_intervals = int(config_dict["max_intervals"])
    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Shutdown target {target_DT} or {max_intervals} intervals.",
        )

    network_name = get_network_name()

    if queues_enabled:
        q_server_port = int(config_dict["q_server_port"])
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")

        if peer_type == "PP":
            queue_server.register("get_want_list_queue")
            queue_server.register("get_provider_queue")
            queue_server.connect()
            out_bound = queue_server.get_want_list_queue()
            in_bound = queue_server.get_provider_queue()

        elif peer_type == "BP":
            queue_server.register("get_bitswap_queue")
            queue_server.connect()
            out_bound = queue_server.get_bitswap_queue()

        elif peer_type == "SP":
            queue_server.register("get_swarm_queue")
            queue_server.connect()
            out_bound = queue_server.get_swarm_queue()
    else:
        out_bound = ""

    capture_interval = 0
    total_found = 0
    total_added = 0
    total_promoted = 0

    current_DT = datetime.now()
    while target_DT > current_DT and capture_interval < max_intervals:
        capture_interval += 1

        msg = f"Start of peer capture interval {capture_interval}"
        if logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=msg,
            )
        if shutdown_query(call_stack):
            break

        status_code, found, added, promoted, duration, released, modified = (
            capture_peers(
                call_stack,
                config_dict,
                queues_enabled,
                peer_type,
                network_name,
                out_bound,
                logging_enabled,
            )
        )

        if status_code != 200:
            if logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg="Capture peers failed.",
                )

        if shutdown_query(call_stack):
            break

        total_found += found
        total_added += added
        total_promoted += promoted

        if released > 0:
            if queues_enabled:
                out_bound.put_nowait("wake up")

        msg = f"Interval {capture_interval} complete with find provider duration {duration} and {released} released."
        if logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=msg,
            )

        if queues_enabled:
            try:
                in_bound.get(timeout=int(config_dict["capture_interval_delay"]))
            except Empty:
                pass
        else:
            sleep(int(config_dict["capture_interval_delay"]))

        if shutdown_query(call_stack):
            break
        current_DT = datetime.now()

    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"{total_found} {peer_type} found, {total_promoted} promoted and {total_added} added in {capture_interval} intervals)",
        )

    add_log(
        process=call_stack,
        peer_type="status",
        msg="Capture Providers complete.",
    )
    return


def capture_peers(
    call_stack,
    config_dict,
    queues_enabled,
    peer_type,
    network_name,
    out_bound: str,
    logging_enabled: bool,
):
    duration = datetime.fromisoformat(get_DTS())
    call_stack = call_stack + ":capture_peers"
    found = 0
    added = 0
    promoted = 0
    released = 0
    modified = 0

    if peer_type == "PP":
        response, status_code, response_dict = execute_request(
            url_key="id",
            call_stack=call_stack,
            http_500_ignore=False,
        )
        if status_code == 200:
            self = response_dict["ID"]  # TODO: replace with LP
        else:
            add_log(
                process=call_stack,
                peer_type="Error",
                msg="Capture Peers self ID failed with ignore.",
            )

            return status_code, found, added, promoted, duration, released, modified

        start_DTS = get_DTS()

        response, status_code, response_dict = execute_request(
            url_key="find_providers",
            param={"arg": network_name},
            call_stack=call_stack,
            http_500_ignore=False,
        )
        if status_code != 200 and status_code != 500:
            add_log(
                process=call_stack,
                peer_type="Error",
                msg="Capture Peers find providers failed.",
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
                    config_dict,
                    queues_enabled,
                    response,
                    self,
                    out_bound,
                    logging_enabled,
                )
            )
            if status_code != 200:
                add_log(
                    process=call_stack,
                    peer_type="Error",
                    msg="Capture Peers from decode findprovs failed.",
                )

    elif peer_type == "BP":
        response, status_code, response_dict = execute_request(
            url_key="bitswap_stat",
            param={"arg": network_name},
            call_stack=call_stack,
        )
        # TODO: 700 Later
        found, added, promoted = decode_bitswap_stat_structure(
            # conn,
            # queries,
            response,
            out_bound,
            # Uconn,
            # Uqueries,
            # Rconn,
            # Rqueries,
            queues_enabled,
        )

    elif peer_type == "SP":
        response, status_code, response_dict = execute_request(
            url_key="swarm_peers",
            param={"arg": network_name},
            call_stack=call_stack,
        )
        # TODO: 700 later
        found, added, promoted = decode_swarm_structure(
            # conn,
            # queries,
            response,
            out_bound,
            # Uconn,
            # Uqueries,
            # Rconn,
            # Rqueries,
            queues_enabled,
        )

    return status_code, found, added, promoted, duration, released, modified


def decode_findprovs_structure(
    call_stack,
    config_dict,
    queues_enabled,
    response,
    self,
    out_bound: str,
    logging_enabled: bool,
):
    status_code = 200
    found = 0
    added = 0
    promoted = 0
    modified = 0
    released = 0
    peer_type = "PP"
    # address_wait_is_enabled = 1
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"
    engine = create_engine(db_url, echo=False, connect_args={"timeout": 120})

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
                # address_available = False
                list_of_peer_dict = str(response).replace(
                    "'", '"'
                )  # needed to use json
                peer_dict = json.loads(str(list_of_peer_dict))
                peer_ID = peer_dict["ID"]
                if self != peer_ID:
                    address_list = peer_dict["Addrs"]

                    peer_table_dict = refresh_peer_row_from_template()
                    # peer_table_dict["peer_ID"] = peer_dict["ID"]
                    # peer_table_dict["local_update_DTS"] = get_DTS()
                    # peer_table_dict["peer_type"] = peer_type
                    # peer_table_dict["disabled"] = 1

                    statement = select(Peer_Table).where(Peer_Table.peer_ID == peer_ID)
                    with Session(engine) as session:
                        results = session.exec(statement)
                        try:
                            current_peer = results.one()
                            original_peer_type = current_peer.peer_type
                            if current_peer.processing_status == "WLW":
                                status_code, address_available, disabled = (
                                    capture_provider_addresses(
                                        call_stack, address_list, peer_ID, peer_type
                                    )
                                )
                                if not disabled and address_available:
                                    current_peer.processing_status = "WLR"
                                    current_peer.local_update_DTS = (get_DTS(),)
                                    current_peer.disabled = 0
                                    session.add(current_peer)
                                    session.commit()
                                    modified += 1
                                    released += 1

                        except NoResultFound:
                            original_peer_type = peer_type
                            status_code, address_available, disabled = (
                                capture_provider_addresses(
                                    call_stack, address_list, peer_ID, peer_type
                                )
                            )
                            if disabled or not address_available:
                                peer_table_row = Peer_Table(
                                    peer_ID=peer_ID,
                                    local_update_DTS=get_DTS(),
                                    origin_update_DTS=get_DTS(),
                                    peer_type=peer_type,
                                    original_peer_type=peer_type,
                                    processing_status="WLW",
                                    disabled=0,
                                )
                            else:
                                peer_table_row = Peer_Table(
                                    peer_ID=peer_ID,
                                    local_update_DTS=get_DTS(),
                                    origin_update_DTS=get_DTS(),
                                    peer_type=peer_type,
                                    original_peer_type=peer_type,
                                    processing_status="WLR",
                                    disabled=0,
                                )
                                released += 1
                            try:
                                session.add(peer_table_row)
                                session.commit()
                                added += 1

                            except IntegrityError:
                                # already released
                                pass

                        if original_peer_type == "BP":
                            # promote
                            # BP -> PP
                            conn, queries = set_up_sql_operations(config_dict)  # +1
                            update_peer_table_peer_type_BP_to_PP(
                                conn, queries, peer_table_dict
                            )
                            conn.commit()
                            conn.close()

                            modified += 1
                            promoted += 1

                        elif original_peer_type == "SP":
                            # promote
                            # SP -> PP
                            conn, queries = set_up_sql_operations(config_dict)  # +1
                            update_peer_table_peer_type_SP_to_PP(
                                conn, queries, peer_table_dict
                            )
                            conn.commit()
                            conn.close()

                            modified += 1
                            promoted += 1

                    if original_peer_type == "PP":  # this should only occur on released
                        # if not disabled:
                        if logging_enabled:
                            msg = "put wake up from PP peer capture"
                            # add_log(
                            # process=call_stack,
                            # peer_type="status",
                            # msg=msg,
                            # )

                    elif original_peer_type == "BP":
                        if logging_enabled:
                            msg = "put promoted from bitswap wake up from PP peer capture"  # released and promoted
                            add_log(
                                process=call_stack,
                                peer_type="status",
                                msg=msg,
                            )

                    elif original_peer_type == "SP":
                        if logging_enabled:
                            msg = "put promoted from swarm wake up from PP peer capture"  # released and promoted
                            add_log(
                                process=call_stack,
                                peer_type="status",
                                msg=msg,
                            )

                else:
                    if logging_enabled:
                        msg = "Local peer was identified as a provider"
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=msg,
                        )

                    address_available = True

                if address_available:
                    break  # break to next provider

    log_string = f"{found} providers found, {added} added, released {released} and {promoted} promoted."
    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=log_string,
        )

    return status_code, found, added, promoted, released, modified


def decode_bitswap_stat_structure(
    # conn,
    # queries,
    r,
    out_bound,
    # Uconn,
    # Uqueries,
    # Rconn,
    # Rqueries,
    queues_enabled,
):
    config_dict = get_provider_capture_config_dict()
    found = 0
    added = 0
    promoted = 0
    p = psutil.Process()
    pid = p.pid

    json_dict = json.loads(r.text)
    peer_list = json_dict["Peers"]
    for peer in peer_list:
        peer_table_dict = refresh_peer_row_from_template()
        DTS = get_DTS()
        peer_table_dict["peer_ID"] = peer
        peer_table_dict["local_update_DTS"] = DTS
        peer_table_dict["peer_type"] = "BP"
        peer_table_dict["processing_status"] = (
            "WLR"  # BP and SP will continue processing until they exceed the
        )
        peer_table_dict["disabled"] = 0
        # zero want list threshold limit
        conn, queries = set_up_sql_operations(config_dict)  # +1
        try:
            insert_peer_row(conn, queries, peer_table_dict)
            conn.commit()
            conn.close()

            added += 1
        except IntegrityError:
            conn.close()
            pass
        found += 1
    # if queues_enabled:
    #    out_bound.put_nowait("put wake up from BP peer capture")

    msg = "put wake up from BP peer capture"
    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "provider_capture_decode_bitswap-1"
    log_dict["pid"] = pid
    log_dict["peer_type"] = "BP"
    log_dict["msg"] = msg
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()

    log_string = f"{found} bitswap found and {added} added."
    msg = log_string
    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "provider_capture_decode_bitswap-2"
    log_dict["pid"] = pid
    log_dict["peer_type"] = "BP"
    log_dict["msg"] = msg
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()
    return found, added, promoted


def decode_swarm_structure(
    # conn,
    # queries,
    r,
    out_bound,
    # Uconn,
    # Uqueries,
    # Rconn,
    # Rqueries,
    queues_enabled,
):
    config_dict = get_provider_capture_config_dict()
    level_zero_dict = json.loads(r.text)
    level_one_list = level_zero_dict["Peers"]
    found = 0
    added = 0
    promoted = 0
    p = psutil.Process()
    pid = p.pid
    for peer_dict in level_one_list:
        peer_table_dict = refresh_peer_row_from_template()
        DTS = get_DTS()
        peer_table_dict["peer_ID"] = peer_dict["Peer"]
        peer_table_dict["local_update_DTS"] = DTS
        peer_table_dict["peer_type"] = "SP"
        peer_table_dict["processing_status"] = "WLR"
        peer_table_dict["disabled"] = 0
        try:
            conn, queries = set_up_sql_operations(config_dict)  # +1
            insert_peer_row(conn, queries, peer_table_dict)
            conn.commit()
            conn.close()
            added += 1

        except IntegrityError:
            pass
        found += 1
    # if queues_enabled:
    #    out_bound.put_nowait("put wake up from SP peer capture")

    msg = "put wake up from SP peer capture"
    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "provider_capture_decode_swarm-1"
    log_dict["pid"] = pid
    log_dict["peer_type"] = "SP"
    log_dict["msg"] = msg
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()

    log_string = f"{found} bitswap found and {added} added."
    msg = log_string
    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "provider_capture_decode_swarm-2"
    log_dict["pid"] = pid
    log_dict["peer_type"] = "SP"
    log_dict["msg"] = msg
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()

    return found, added, promoted


def capture_provider_addresses(
    call_stack: str,
    address_list: list,
    peer_ID: str,
    logging_enabled: bool,
) -> bool:
    call_stack = call_stack + ":capture_provider_addresses"
    address_available = False
    status_code = 800
    address_source = "PP"
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"
    engine = create_engine(db_url, echo=False, connect_args={"timeout": 120})

    capture_addresses(  # provider is source
        call_stack,
        address_list,
        peer_ID,
        address_source,
    )

    param = {"arg": peer_ID}
    response, status_code, response_dict = execute_request(  # gather peer addresses
        url_key="id",
        param=param,
        call_stack=call_stack,
        http_500_ignore=False,
        connect_retries=1,
    )
    # statement_2 = (
    #        select(Peer_Table)
    #        .where(Peer_Table.peer_ID == peer_ID)
    #        )
    if status_code == 200:
        address_source = "FP"
        peer_dict = json.loads(response.text)
        address_list = peer_dict["Addresses"]
        capture_addresses(
            call_stack,
            address_list,
            peer_ID,
            address_source,
        )
        disabled = 0
        # with Session(engine) as session:
        #    results = session.exec(statement_2)#TODO: move to provider decode
        #    try:
        #        current_peer = results.one()
        #        current_peer.disabled = 0
        #        session.add(current_peer)
        #        session.commit()
        #    except NoResultFound:
        #        pass
    else:
        disabled = 1
        # with Session(engine) as session:
        #    results = session.exec(statement_2)
        #    try:
        #        current_peer = results.one()
        #        current_peer.disabled = 1
        #        session.add(current_peer)
        #        session.commit()
        #        msg="ID failed and peer disabled."
        #    except NoResultFound:
        msg = "ID request failed. Peer most likely unreachable."
        if logging_enabled:
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

    status_code = 200

    return status_code, address_available, disabled


def capture_addresses(
    call_stack: str, address_list: list, peer_ID: str, address_source: str
) -> bool:
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
) -> None:
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"

    engine = create_engine(db_url, echo=False, connect_args={"timeout": 120})

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

    os.environ["DIYIMS_ROAMING"] = "RoamingDev"
    os.environ["COMPONENT_TEST"] = "1"
    os.environ["QUEUES_ENABLED"] = "0"

    provider_capture_main("__main__", "PP")
