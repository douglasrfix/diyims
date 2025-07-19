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

import json
import ipaddress
import psutil
from queue import Empty
from diyims.requests_utils import execute_request
from datetime import datetime
from sqlite3 import IntegrityError
from time import sleep
from multiprocessing.managers import BaseManager
from sqlmodel import create_engine, Session, select
from diyims.ipfs_utils import get_url_dict
from diyims.database_utils import (
    insert_peer_row,
    refresh_peer_row_from_template,
    select_peer_table_entry_by_key,
    set_up_sql_operations,
    refresh_log_dict,
    insert_log_row,
    update_peer_table_status_WLW_to_WLR,
    # update_peer_table_version,
    update_peer_table_peer_type_BP_to_PP,
    update_peer_table_peer_type_SP_to_PP,
    select_shutdown_entry,
)
from diyims.general_utils import get_network_name, get_shutdown_target, get_DTS
from diyims.logger_utils import get_logger
from diyims.config_utils import get_capture_peer_config_dict
from diyims.path_utils import get_path_dict
from diyims.sqlmodels import Peer_Address

#  psutil.BELOW_NORMAL_PRIORITY_CLASS,
#  psutil.NORMAL_PRIORITY_CLASS,
#  psutil.ABOVE_NORMAL_PRIORITY_CLASS,
#  psutil.HIGH_PRIORITY_CLASS,
#  psutil.REALTIME_PRIORITY_CLASS

# class Peer_Address(SQLModel, table=True):
#    peer_ID: str = Field(primary_key=True)
#    multiaddress: str = Field(primary_key=True)
#    insert_timestamp: str | None = None


def capture_peer_main(peer_type):
    p = psutil.Process()

    pid = p.pid
    capture_peer_config_dict = get_capture_peer_config_dict()
    logger = get_logger(capture_peer_config_dict["log_file"], peer_type)
    wait_seconds = int(capture_peer_config_dict["wait_before_startup"])
    logger.debug(f"Waiting for {wait_seconds} seconds before startup.")
    sleep(wait_seconds)  # config value
    if peer_type == "PP":
        # p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # TODO: put in config

        logger.info("Provider Capture startup.")
    elif peer_type == "BP":
        # p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # TODO: put in config

        logger.info("Startup of Bitswap Capture.")
    elif peer_type == "SP":
        # p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # TODO: put in config

        logger.info("Startup of Swarm Capture.")
    interval_length = int(capture_peer_config_dict["capture_interval_delay"])
    target_DT = get_shutdown_target(capture_peer_config_dict)
    max_intervals = int(capture_peer_config_dict["max_intervals"])
    logger.debug(
        f"Shutdown target {target_DT} or {max_intervals} intervals of {interval_length} seconds."
    )
    url_dict = get_url_dict()
    network_name = get_network_name()
    q_server_port = int(capture_peer_config_dict["q_server_port"])
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

    capture_interval = 0
    total_found = 0
    total_added = 0
    total_promoted = 0
    current_DT = datetime.now()
    while target_DT > current_DT and capture_interval < max_intervals:
        capture_interval += 1
        # logger.debug(f"Start of Interval {capture_interval}")
        msg = f"Start of peer capture interval {capture_interval}"
        log_dict = (
            refresh_log_dict()
        )  # TODO: rename template  maybe create a function to condense this
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "peer_capture_main-1"
        log_dict["pid"] = pid
        log_dict["peer_type"] = peer_type
        log_dict["msg"] = msg
        conn, queries = set_up_sql_operations(capture_peer_config_dict)  # +1
        insert_log_row(conn, queries, log_dict)
        conn.commit()
        conn.close()
        conn, queries = set_up_sql_operations(capture_peer_config_dict)  # +1
        shutdown_row_dict = select_shutdown_entry(
            conn,
            queries,
        )
        conn.close()
        if shutdown_row_dict["enabled"]:
            break

        found, added, promoted, duration, modified = capture_peers(
            logger,
            # conn,
            # queries,
            capture_peer_config_dict,
            url_dict,
            out_bound,
            peer_type,
            network_name,
            # Uconn,
            # Uqueries,
            # Rconn,
            # Rqueries,
        )
        conn, queries = set_up_sql_operations(capture_peer_config_dict)  # +1
        shutdown_row_dict = select_shutdown_entry(
            conn,
            queries,
        )
        conn.close()
        if shutdown_row_dict["enabled"]:
            break

        total_found += found
        total_added += added
        total_promoted += promoted

        if modified > 0:
            out_bound.put_nowait("wake up")

        msg = f"Interval {capture_interval} complete with find provider duration {duration}."
        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "peer_capture_main-2"
        log_dict["pid"] = pid
        log_dict["peer_type"] = peer_type
        log_dict["msg"] = msg
        conn, queries = set_up_sql_operations(capture_peer_config_dict)  # +1
        insert_log_row(conn, queries, log_dict)
        conn.commit()
        conn.close()

        try:
            in_bound.get(
                timeout=int(capture_peer_config_dict["capture_interval_delay"])
            )
            conn, queries = set_up_sql_operations(capture_peer_config_dict)  # +1
            shutdown_row_dict = select_shutdown_entry(
                conn,
                queries,
            )
            conn.close()
            if shutdown_row_dict["enabled"]:
                break
        except Empty:
            pass
        current_DT = datetime.now()

    log_string = f"{total_found} {peer_type} found, {total_promoted} promoted and {total_added} added in {capture_interval} intervals)"
    logger.info(log_string)

    logger.info("Provider Capture shutdown.")
    return


def capture_peers(
    logger,
    # conn,
    # queries,
    capture_peer_config_dict,
    url_dict,
    out_bound,
    peer_type,
    network_name,
    # Uconn,
    # Uqueries,
    # Rconn,
    # Rqueries,
):
    duration = datetime.fromisoformat(get_DTS())

    if peer_type == "PP":
        response, status_code, response_dict = execute_request(
            url_key="id",
            logger=logger,
            url_dict=url_dict,
            config_dict=capture_peer_config_dict,
        )

        self = response_dict["ID"]
        start_DTS = get_DTS()

        response, status_code, response_dict = execute_request(
            url_key="find_providers",
            logger=logger,
            url_dict=url_dict,
            config_dict=capture_peer_config_dict,
            param={"arg": network_name},
        )

        if status_code == 200:
            stop_DTS = get_DTS()
            start = datetime.fromisoformat(start_DTS)
            stop = datetime.fromisoformat(stop_DTS)
            duration = stop - start

            found, added, promoted, modified = decode_findprovs_structure(
                logger,
                # conn,
                # queries,
                capture_peer_config_dict,
                url_dict,
                response,
                out_bound,
                # Uconn,
                # Uqueries,
                # ,
                # Rqueries,
                self,
            )

    elif peer_type == "BP":
        response, status_code, response_dict = execute_request(
            url_key="bitswap_stat",
            logger=logger,
            url_dict=url_dict,
            config_dict=capture_peer_config_dict,
            param={"arg": network_name},
        )

        found, added, promoted = decode_bitswap_stat_structure(
            # conn,
            # queries,
            response,
            out_bound,
            # Uconn,
            # Uqueries,
            # Rconn,
            # Rqueries,
        )

    elif peer_type == "SP":
        response, status_code, response_dict = execute_request(
            url_key="swarm_peers",
            logger=logger,
            url_dict=url_dict,
            config_dict=capture_peer_config_dict,
            param={"arg": network_name},
        )

        found, added, promoted = decode_swarm_structure(
            # conn,
            # queries,
            response,
            out_bound,
            # Uconn,
            # Uqueries,
            # Rconn,
            # Rqueries,
        )

    return found, added, promoted, duration, modified


def decode_findprovs_structure(
    logger,
    # conn,
    # queries,
    capture_peer_config_dict,
    url_dict,
    response,
    out_bound,
    # Uconn,
    # Uqueries,
    # Rconn,
    # Rqueries,
    self,
):
    found = 0
    added = 0
    promoted = 0
    modified = 0
    p = psutil.Process()
    pid = p.pid
    address_wait_is_enabled = 1

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
                    if capture_provider_addresses(address_list, peer_ID):
                        address_available = True

                    peer_table_dict = refresh_peer_row_from_template()
                    peer_table_dict["peer_ID"] = peer_dict["ID"]
                    peer_table_dict["local_update_DTS"] = get_DTS()
                    peer_table_dict["peer_type"] = "PP"

                    if address_available:  # set initial value and move on
                        peer_table_dict["processing_status"] = "WLR"
                        # peer_table_dict["version"] = connect_address
                    else:
                        #  always wait for addresses
                        if address_wait_is_enabled:
                            peer_table_dict["processing_status"] = "WLW"
                        else:
                            peer_table_dict["processing_status"] = "WLR"
                        # peer_table_dict["version"] = "0"
                    conn, queries = set_up_sql_operations(
                        capture_peer_config_dict
                    )  # +1
                    try:  # TODO: test here for existing peer first
                        insert_peer_row(conn, queries, peer_table_dict)
                        conn.commit()
                        conn.close()
                        # out_bound.put_nowait("wake up")
                        if address_available:
                            modified += 1

                        original_peer_type = peer_table_dict["peer_type"]

                    except IntegrityError:
                        # conn, queries = set_up_sql_operations(capture_peer_config_dict)  # +1
                        conn.rollback()
                        peer_table_entry = select_peer_table_entry_by_key(
                            conn, queries, peer_table_dict
                        )
                        conn.close()

                        original_peer_type = peer_table_entry["peer_type"]
                        peer_table_dict["peer_ID"] = peer_table_entry["peer_id"]
                        commit_peer = 0
                        if peer_table_entry["processing_status"] == "WLW":
                            # if (
                            #    peer_table_entry["version"] == "0"
                            # ):  # exiting entry waiting on address
                            if address_available:
                                # address is available, wait no longer
                                # peer_table_dict["processing_status"] = "WLR"
                                # peer_table_dict["version"] = connect_address

                                # WLW -> WLR
                                conn, queries = set_up_sql_operations(
                                    capture_peer_config_dict
                                )  # +1
                                update_peer_table_status_WLW_to_WLR(
                                    conn, queries, peer_table_dict
                                )
                                conn.commit()
                                conn.close()

                                modified += 1
                                commit_peer = 1
                            """
                            else:
                                if (
                                    peer_table_entry["version"]
                                    != peer_table_dict["version"]
                                ):
                                    peer_table_dict["version"] = peer_table_entry[
                                        "version"
                                    ]

                                    # provide new address to help connect failing

                                    update_peer_table_version(
                                        conn, queries, peer_table_dict
                                    )
                                    # conn.commit()

                                    modified += 1
                                    commit_peer = 1"""

                        if original_peer_type == "BP":
                            # promote
                            # BP -> PP
                            conn, queries = set_up_sql_operations(
                                capture_peer_config_dict
                            )  # +1
                            update_peer_table_peer_type_BP_to_PP(
                                conn, queries, peer_table_dict
                            )
                            conn.commit()
                            conn.close()

                            commit_peer = 1
                            modified += 1
                            promoted += 1

                        elif original_peer_type == "SP":
                            # promote
                            # SP -> PP
                            conn, queries = set_up_sql_operations(
                                capture_peer_config_dict
                            )  # +1
                            update_peer_table_peer_type_SP_to_PP(
                                conn, queries, peer_table_dict
                            )
                            conn.commit()
                            conn.close()

                            commit_peer = 1
                            modified += 1
                            promoted += 1

                        elif (
                            original_peer_type == "LP"
                        ):  # added by db-init no longer applies until moved out of logic constrained by self
                            msg = "Local peer was identified as a provider"
                            log_dict = refresh_log_dict()
                            log_dict["DTS"] = get_DTS()
                            log_dict["process"] = "peer_capture_decode_provider-2"
                            log_dict["pid"] = pid
                            log_dict["peer_type"] = "PP"
                            log_dict["msg"] = msg
                            conn, queries = set_up_sql_operations(
                                capture_peer_config_dict
                            )  # +1
                            insert_log_row(conn, queries, log_dict)
                            commit_peer = 1
                            conn.commit()
                            conn.close()

                        if commit_peer:
                            # conn.commit()

                            commit_peer = 0

                    if original_peer_type == "PP":
                        msg = "put wake up from PP peer capture"
                        log_dict = refresh_log_dict()
                        log_dict["DTS"] = get_DTS()
                        log_dict["process"] = "peer_capture_decode_provider-3"
                        log_dict["pid"] = pid
                        log_dict["peer_type"] = "PP"
                        log_dict["msg"] = msg
                        # insert_log_row(conn, queries, log_dict)
                        # conn.commit()

                    elif original_peer_type == "BP":
                        msg = "put promoted from bitswap wake up from PP peer capture"
                        log_dict = refresh_log_dict()
                        log_dict["DTS"] = get_DTS()
                        log_dict["process"] = "peer_capture_decode_provider-4"
                        log_dict["pid"] = pid
                        log_dict["peer_type"] = "PP"
                        log_dict["msg"] = msg
                        conn, queries = set_up_sql_operations(
                            capture_peer_config_dict
                        )  # +1
                        insert_log_row(conn, queries, log_dict)
                        conn.commit()
                        conn.close()

                    elif original_peer_type == "SP":
                        msg = "put promoted from swarm wake up from PP peer capture"
                        log_dict = refresh_log_dict()
                        log_dict["DTS"] = get_DTS()
                        log_dict["process"] = "peer_capture_decode_provider-5"
                        log_dict["pid"] = pid
                        log_dict["peer_type"] = "PP"
                        log_dict["msg"] = msg
                        conn, queries = set_up_sql_operations(
                            capture_peer_config_dict
                        )  # +1
                        insert_log_row(conn, queries, log_dict)
                        conn.commit()
                        conn.close()

                if address_available:
                    break  # break to next provider

    log_string = f"{found} providers found, {added} added and {promoted} promoted."

    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "peer_capture_decode_provider-6"
    log_dict["pid"] = pid
    log_dict["peer_type"] = "PP"
    log_dict["msg"] = log_string
    conn, queries = set_up_sql_operations(capture_peer_config_dict)  # +1
    insert_log_row(conn, queries, log_dict)
    conn.commit()
    conn.close()

    return found, added, promoted, modified


def decode_bitswap_stat_structure(
    # conn,
    # queries,
    r,
    out_bound,
    # Uconn,
    # Uqueries,
    # Rconn,
    # Rqueries,
):
    config_dict = get_capture_peer_config_dict()
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

    out_bound.put_nowait("put wake up from BP peer capture")

    msg = "put wake up from BP peer capture"
    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "peer_capture_decode_bitswap-1"
    log_dict["pid"] = pid
    log_dict["peer_type"] = "BP"
    log_dict["msg"] = msg
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()

    log_string = f"{found} bitswap found and {added} added."
    msg = log_string
    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "peer_capture_decode_bitswap-2"
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
):
    config_dict = get_capture_peer_config_dict()
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
        try:
            conn, queries = set_up_sql_operations(config_dict)  # +1
            insert_peer_row(conn, queries, peer_table_dict)
            conn.commit()
            conn.close()
            added += 1

        except IntegrityError:
            pass
        found += 1

    out_bound.put_nowait("put wake up from SP peer capture")

    msg = "put wake up from SP peer capture"
    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "peer_capture_decode_swarm-1"
    log_dict["pid"] = pid
    log_dict["peer_type"] = "SP"
    log_dict["msg"] = msg
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()

    log_string = f"{found} bitswap found and {added} added."
    msg = log_string
    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "peer_capture_decode_swarm-2"
    log_dict["pid"] = pid
    log_dict["peer_type"] = "SP"
    log_dict["msg"] = msg
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()

    return found, added, promoted


def capture_provider_addresses(address_list: list, peer_ID: str) -> bool:
    address_available = False

    if capture_peer_addresses(address_list, peer_ID):
        address_available = True
    param = {"arg": peer_ID}
    response, status_code, response_dict = execute_request(
        url_key="id",
        param=param,
    )
    if status_code == 200:
        peer_dict = json.loads(response.text)
        address_list = peer_dict["Addresses"]
        if capture_peer_addresses(address_list, peer_ID):
            address_available = True

    return address_available


def capture_peer_addresses(address_list: list, peer_ID: str) -> bool:
    address_available = False
    for address in address_list:
        address = address
        multiaddress = ""
        address_suspect = False
        index = address.lower().find(
            "/p2p-circuit"
        )  # most often observed in data order not really of interest
        if index == -1:
            index = address.lower().find("/web")
            if index == -1:
                index = address.lower().find("/dns")
                if index == -1:
                    index = address.lower().find("/tls")  # this might be ok
                    if index == -1:
                        ip_version = address[:5]
                        index = address.lower().find("/", 5)
                        ip_string = address[5:index]
                        ip_index = index

                        index = address.lower().find(
                            "/p2p/", ip_index
                        )  # if index != to -1 then it is assumed to be multiaddress
                        if index != -1:
                            multiaddress = address
                        else:
                            index = address.lower().find("/quic-v1", ip_index)
                            if index != -1:
                                multiaddress = address + "/p2p/" + peer_ID
                            else:
                                if index == -1:
                                    index = address.lower().find(
                                        "/tcp/", ip_index
                                    )  # if index != to -1 then it is assumed to be multiaddress
                                if (
                                    index == -1
                                ):  # probably a findprovs address which are not multiaddress format
                                    index = address.lower().find("/udp/", ip_index)
                                if index != -1:
                                    port_start = index + 5
                                    port = address[port_start:]
                                    if port.isnumeric():
                                        multiaddress = address + "/p2p/" + peer_ID
                                    else:
                                        multiaddress = address
                                        address_suspect = True
                                if multiaddress != "":
                                    if ip_version == "/ip4/":
                                        if ipaddress.IPv4Address(ip_string).is_global:
                                            insert_DTS = get_DTS()
                                            # print(index, ip_version, ip_string, address)

                                            create_peer_address(
                                                peer_ID,
                                                multiaddress,
                                                insert_DTS,
                                                address_suspect,
                                            )
                                            address_available = True

                                    else:
                                        if ipaddress.IPv6Address(ip_string).is_global:
                                            insert_DTS = get_DTS()
                                            # print(index, ip_version, ip_string, address)
                                            create_peer_address(
                                                peer_ID,
                                                multiaddress,
                                                insert_DTS,
                                                address_suspect,
                                            )
                                            address_available = True

    return address_available


def create_peer_address(
    peer_ID: str, multiaddress: str, insert_DTS: str, address_suspect: bool
) -> None:
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"

    engine = create_engine(db_url, echo=False, connect_args={"timeout": 120})
    # session = Session(engine)
    # statement = text("PRAGMA busy_timeout = 100000;")
    # session.exec(statement)

    address_row = Peer_Address(
        peer_ID=peer_ID,
        multiaddress=multiaddress,
        insert_DTS=insert_DTS,
        address_suspect=address_suspect,
    )
    with Session(engine) as session:
        statement = select(Peer_Address).where(
            Peer_Address.peer_ID == peer_ID, Peer_Address.multiaddress == multiaddress
        )
        results = session.exec(statement).first()
        if results is None:
            session.add(address_row)
            session.commit()
    return


if __name__ == "__main__":
    capture_peer_main("PP")
