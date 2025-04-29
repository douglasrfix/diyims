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
import psutil
from diyims.requests_utils import execute_request
from datetime import datetime
from sqlite3 import IntegrityError
from time import sleep
from multiprocessing.managers import BaseManager
from diyims.ipfs_utils import get_url_dict
from diyims.database_utils import (
    insert_peer_row,
    refresh_peer_row_from_template,
    select_peer_table_entry_by_key,
    update_peer_table_peer_type_status,
    set_up_sql_operations,
    refresh_log_dict,
    insert_log_row,
)
from diyims.general_utils import get_network_name, get_shutdown_target, get_DTS
from diyims.logger_utils import get_logger
from diyims.config_utils import get_capture_peer_config_dict

#  psutil.BELOW_NORMAL_PRIORITY_CLASS,
#  psutil.NORMAL_PRIORITY_CLASS,
#  psutil.ABOVE_NORMAL_PRIORITY_CLASS,
#  psutil.HIGH_PRIORITY_CLASS,
#  psutil.REALTIME_PRIORITY_CLASS


def capture_peer_main(peer_type):
    p = psutil.Process()

    pid = p.pid
    capture_peer_config_dict = get_capture_peer_config_dict()
    logger = get_logger(capture_peer_config_dict["log_file"], peer_type)
    wait_seconds = int(capture_peer_config_dict["wait_before_startup"])
    logger.debug(f"Waiting for {wait_seconds} seconds before startup.")
    sleep(wait_seconds)
    if peer_type == "PP":
        # p.nice(psutil.NORMAL_PRIORITY_CLASS)  # TODO: put in config

        logger.info("Startup of Provider Capture.")
    elif peer_type == "BP":
        # p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # TODO: put in config

        logger.info("Startup of Bitswap Capture.")
    elif peer_type == "SP":
        # p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # TODO: put in config

        logger.info("Startup of Swarm Capture.")
    interval_length = int(capture_peer_config_dict["capture_interval_delay"])
    target_DT = get_shutdown_target(capture_peer_config_dict)
    max_intervals = int(capture_peer_config_dict["max_intervals"])
    logger.info(
        f"Shutdown target {target_DT} or {max_intervals} intervals of {interval_length} seconds."
    )
    url_dict = get_url_dict()
    network_name = get_network_name()
    q_server_port = int(capture_peer_config_dict["q_server_port"])
    queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
    if peer_type == "PP":
        queue_server.register("get_provider_queue")
        queue_server.connect()
        peer_queue = queue_server.get_provider_queue()

    elif peer_type == "BP":
        queue_server.register("get_bitswap_queue")
        queue_server.connect()
        peer_queue = queue_server.get_bitswap_queue()

    elif peer_type == "SP":
        queue_server.register("get_swarm_queue")
        queue_server.connect()
        peer_queue = queue_server.get_swarm_queue()

    capture_interval = 0
    total_found = 0
    total_added = 0
    total_promoted = 0
    current_DT = datetime.now()
    while target_DT > current_DT and capture_interval < max_intervals:
        conn, queries = set_up_sql_operations(capture_peer_config_dict)  # +1
        Uconn, Uqueries = set_up_sql_operations(capture_peer_config_dict)  # +1
        Rconn, Rqueries = set_up_sql_operations(capture_peer_config_dict)  # +1

        capture_interval += 1
        # logger.debug(f"Start of Interval {capture_interval}")
        msg = f"Start of peer capture interval {capture_interval}"
        log_dict = (
            refresh_log_dict()
        )  # TODO: rename template  maybe create a function to condense this
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "peer_capture_main=1"
        log_dict["pid"] = pid
        log_dict["peer_type"] = peer_type
        log_dict["msg"] = msg
        insert_log_row(conn, queries, log_dict)
        conn.commit()

        found, added, promoted = capture_peers(
            logger,
            conn,
            queries,
            capture_peer_config_dict,
            url_dict,
            peer_queue,
            peer_type,
            network_name,
            Uconn,
            Uqueries,
            Rconn,
            Rqueries,
        )

        total_found += found
        total_added += added
        total_promoted += promoted

        msg = f"Interval {capture_interval} complete."
        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "peer_capture_main-2"
        log_dict["pid"] = pid
        log_dict["peer_type"] = peer_type
        log_dict["msg"] = msg
        insert_log_row(conn, queries, log_dict)
        conn.commit()

        conn.close()  # -1
        Uconn.close()
        Rconn.close()
        # logger.debug(f"Interval {capture_interval} complete.")
        sleep(int(capture_peer_config_dict["capture_interval_delay"]))
        current_DT = datetime.now()

    log_string = f"{total_found} {peer_type} found, {total_promoted} promoted and {total_added} added in {capture_interval} intervals)"
    logger.info(log_string)

    logger.info("Normal shutdown of Capture Process.")
    return


def capture_peers(
    logger,
    conn,
    queries,
    capture_peer_config_dict,
    url_dict,
    peer_queue,
    peer_type,
    network_name,
    Uconn,
    Uqueries,
    Rconn,
    Rqueries,
):
    if peer_type == "PP":
        response, status_code, response_dict = execute_request(
            url_key="find_providers",
            logger=logger,
            url_dict=url_dict,
            config_dict=capture_peer_config_dict,
            param={"arg": network_name},
        )

        found, added, promoted = decode_findprovs_structure(
            logger,
            conn,
            queries,
            capture_peer_config_dict,
            url_dict,
            response,
            peer_queue,
            Uconn,
            Uqueries,
            Rconn,
            Rqueries,
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
            conn,
            queries,
            response,
            peer_queue,
            Uconn,
            Uqueries,
            Rconn,
            Rqueries,
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
            conn,
            queries,
            response,
            peer_queue,
            Uconn,
            Uqueries,
            Rconn,
            Rqueries,
        )

    return found, added, promoted


def decode_findprovs_structure(
    logger,
    conn,
    queries,
    capture_peer_config_dict,
    url_dict,
    response,
    peer_queue,
    Uconn,
    Uqueries,
    Rconn,
    Rqueries,
):
    found = 0
    added = 0
    promoted = 0
    p = psutil.Process()
    pid = p.pid

    for line in response.iter_lines():  # TODO: make this return a list to be processed by a database function to reduce if else nesting
        if line:
            decoded_line = line.decode("utf-8")
            line_dict = json.loads(decoded_line)
            if line_dict["Type"] == 4:
                responses_string = str(line_dict["Responses"])
                responses_string_len = len(responses_string)
                trimmed_responses_string = responses_string[
                    1 : responses_string_len - 1
                ]
                responses_dict = json.loads(trimmed_responses_string.replace("'", '"'))
                addrs_list = responses_dict["Addrs"]
                try:
                    peer_address = addrs_list[0]
                    peer_address = peer_address
                    # the source for this is dht which may be present with out an address
                    address_available = True

                except IndexError:
                    log_dict = refresh_log_dict()
                    log_dict["DTS"] = get_DTS()
                    log_dict["process"] = "peer_capture_decode_provider-1"
                    log_dict["pid"] = pid
                    log_dict["peer_type"] = "PP"
                    log_dict["msg"] = "No Address 1"
                    # insert_log_row(conn, queries, log_dict)
                    # conn.commit()
                    address_available = True

                found += 1

                if address_available:
                    peer_table_dict = refresh_peer_row_from_template()
                    DTS = get_DTS()
                    peer_table_dict["peer_ID"] = responses_dict["ID"]
                    peer_table_dict["local_update_DTS"] = DTS
                    peer_table_dict["peer_type"] = "PP"

                    try:
                        peer_table_dict["processing_status"] = "WLR"
                        insert_peer_row(conn, Uqueries, peer_table_dict)
                        conn.commit()
                        added += 1
                        # NOTE: connect_flag = True disable connect function permanently if latter version prove more robust
                        # connect_flag = False
                        original_peer_type = peer_table_dict["peer_type"]

                    except IntegrityError:  # NOTE: revisit when python is ? Database
                        # connect_flag = False
                        peer_table_entry = select_peer_table_entry_by_key(
                            Rconn, Rqueries, peer_table_dict
                        )
                        original_peer_type = peer_table_entry["peer_type"]

                    if original_peer_type == "BP":
                        if peer_table_entry["processing_status"] == "WLZ":
                            peer_table_dict["processing_status"] = "WLR"
                        else:
                            peer_table_dict["processing_status"] = peer_table_entry[
                                "processing_status"
                            ]
                        # Uconn, Uqueries = set_up_sql_operations(capture_peer_config_dict)
                        update_peer_table_peer_type_status(
                            conn, Uqueries, peer_table_dict
                        )
                        promoted += 1
                        conn.commit()
                        # Uconn.close()
                        # connect_flag = False
                        # connect_flag = True

                    elif original_peer_type == "SP":
                        if peer_table_entry["processing_status"] == "WLZ":
                            peer_table_dict["processing_status"] = "WLR"
                        else:
                            peer_table_dict["processing_status"] = peer_table_entry[
                                "processing_status"
                            ]
                        # Uconn, Uqueries = set_up_sql_operations(capture_peer_config_dict)
                        update_peer_table_peer_type_status(
                            conn, Uqueries, peer_table_dict
                        )
                        promoted += 1
                        conn.commit()
                        # Uconn.close()
                        # connect_flag = False
                        # connect_flag = True

                    elif original_peer_type == "LP":
                        msg = "Local peer was identified as a provider"
                        log_dict = refresh_log_dict()
                        log_dict["DTS"] = get_DTS()
                        log_dict["process"] = "peer_capture_decode_provider-2"
                        log_dict["pid"] = pid
                        log_dict["peer_type"] = "PP"
                        log_dict["msg"] = msg
                        insert_log_row(conn, queries, log_dict)
                        conn.commit()

                if original_peer_type == "PP":  # wake up every interval for providers
                    peer_queue.put_nowait("put wake up from PP peer capture")

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
                    peer_queue.put_nowait(
                        "put promoted from bitswap wake up from PP peer capture"
                    )

                    msg = "put promoted from bitswap wake up from PP peer capture"
                    log_dict = refresh_log_dict()
                    log_dict["DTS"] = get_DTS()
                    log_dict["process"] = "peer_capture_decode_provider-4"
                    log_dict["pid"] = pid
                    log_dict["peer_type"] = "PP"
                    log_dict["msg"] = msg
                    insert_log_row(conn, queries, log_dict)
                    conn.commit()

                elif original_peer_type == "SP":
                    peer_queue.put_nowait(
                        "put promoted from swarm wake up from PP peer capture"
                    )

                    msg = "put promoted from swarm wake up from PP peer capture"
                    log_dict = refresh_log_dict()
                    log_dict["DTS"] = get_DTS()
                    log_dict["process"] = "peer_capture_decode_provider-5"
                    log_dict["pid"] = pid
                    log_dict["peer_type"] = "PP"
                    log_dict["msg"] = msg
                    insert_log_row(conn, queries, log_dict)
                    conn.commit()

    log_string = f"{found} providers found, {added} added and {promoted} promoted."

    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "peer_capture_decode_provider-6"
    log_dict["pid"] = pid
    log_dict["peer_type"] = "PP"
    log_dict["msg"] = log_string
    insert_log_row(conn, queries, log_dict)
    conn.commit()

    return found, added, promoted


def decode_bitswap_stat_structure(
    conn,
    queries,
    r,
    peer_queue,
    Uconn,
    Uqueries,
    Rconn,
    Rqueries,
):
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
        try:
            insert_peer_row(conn, Uqueries, peer_table_dict)
            conn.commit()

            added += 1
        except IntegrityError:
            pass
        found += 1

    peer_queue.put_nowait("put wake up from BP peer capture")

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
    conn,
    queries,
    r,
    peer_queue,
    Uconn,
    Uqueries,
    Rconn,
    Rqueries,
):
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
            insert_peer_row(conn, Uqueries, peer_table_dict)
            conn.commit()
            added += 1

        except IntegrityError:
            pass
        found += 1

    peer_queue.put_nowait("put wake up from SP peer capture")

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


if __name__ == "__main__":
    capture_peer_main("PP")
