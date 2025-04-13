import json
import psutil
from datetime import datetime, timedelta, timezone
from time import sleep
from sqlite3 import IntegrityError
from multiprocessing import Pool, set_start_method, freeze_support
from multiprocessing.managers import BaseManager
from queue import Empty
from diyims.requests_utils import execute_request
from diyims.database_utils import (
    insert_want_list_row,
    select_want_list_entry_by_key,
    update_last_update_DTS,
    refresh_peer_table_dict,
    refresh_want_list_table_dict,
    set_up_sql_operations,
    update_peer_table_status_WLR,
    update_peer_table_status_WLP,
    update_peer_table_status_WLX,
    update_peer_table_status_WLZ,
    refresh_log_dict,
    insert_log_row,
)
from diyims.general_utils import get_DTS, get_shutdown_target
from diyims.ipfs_utils import get_url_dict
from diyims.logger_utils import get_logger_task
from diyims.config_utils import get_want_list_config_dict


def capture_peer_want_lists(peer_type):  # each peer type runs in its own process
    freeze_support()
    try:
        set_start_method("spawn")
    except RuntimeError:
        pass
    p = psutil.Process()
    p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # NOTE: put in config
    pid = p.pid

    want_list_config_dict = get_want_list_config_dict()
    conn, queries = set_up_sql_operations(want_list_config_dict)
    # logger = get_logger(
    #    want_list_config_dict["log_file"],
    #    peer_type,
    # )
    wait_seconds = int(want_list_config_dict["wait_before_startup"])

    log_string = f"Waiting for {wait_seconds} seconds before startup."
    msg = log_string
    log_dict = refresh_log_dict()
    log_dict["DTS"] = str(datetime.now(timezone.utc))
    log_dict["process"] = "peer_want_capture_main-0"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = msg
    insert_log_row(conn, queries, log_dict)
    conn.commit()

    sleep(wait_seconds)
    log_string = "Startup of Want List Capture."
    msg = log_string
    log_dict = refresh_log_dict()
    log_dict["DTS"] = str(datetime.now(timezone.utc))
    log_dict["process"] = "peer_want_capture_main-0"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = msg
    insert_log_row(conn, queries, log_dict)
    conn.commit()

    target_DT = get_shutdown_target(want_list_config_dict)

    max_intervals = int(want_list_config_dict["max_intervals"])
    number_of_samples_per_interval = int(
        want_list_config_dict["number_of_samples_per_interval"]
    )
    seconds_per_sample = 60 // int(want_list_config_dict["samples_per_minute"])
    total_seconds = number_of_samples_per_interval * seconds_per_sample
    wait_for_new_peer = 60 * int(want_list_config_dict["wait_for_new_peer_minutes"])
    log_string = f"Shutdown target {target_DT} or {max_intervals} intervals of {total_seconds} seconds."

    msg = log_string
    log_dict = refresh_log_dict()
    log_dict["DTS"] = str(datetime.now(timezone.utc))
    log_dict["process"] = "peer_want_capture_main-0"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = msg
    insert_log_row(conn, queries, log_dict)
    conn.commit()

    interval_count = 0
    total_peers_processed = 0
    q_server_port = int(want_list_config_dict["q_server_port"])
    queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
    if peer_type == "PP":
        queue_server.register("get_provider_queue")
        queue_server.connect()
        peer_queue = queue_server.get_provider_queue()
        pool_workers = int(want_list_config_dict["provider_pool_workers"])
        maxtasks = int(want_list_config_dict["provider_maxtasks"])
    elif peer_type == "BP":
        queue_server.register("get_bitswap_queue")
        queue_server.connect()
        peer_queue = queue_server.get_bitswap_queue()
        pool_workers = int(want_list_config_dict["bitswap_pool_workers"])
        maxtasks = int(want_list_config_dict["bitswap_maxtasks"])
    elif peer_type == "SP":
        queue_server.register("get_swarm_queue")
        queue_server.connect()
        peer_queue = queue_server.get_swarm_queue()
        pool_workers = int(want_list_config_dict["swarm_pool_workers"])
        maxtasks = int(want_list_config_dict["swarm_maxtasks"])

    with Pool(processes=pool_workers, maxtasksperchild=maxtasks) as pool:
        # used to throttle how many peers are processed concurrently
        current_DT = datetime.now()
        while target_DT > current_DT:
            # find any available peers that were previously captured before waiting for new ones
            peers_processed = capture_want_lists_for_peers(
                # logger,
                want_list_config_dict,
                peer_type,
                pool,
            )
            total_peers_processed += peers_processed
            log_string = f"{peers_processed} {peer_type} peers submitted for Want List processing."
            # logger.debug(log_string)
            msg = log_string
            log_dict = refresh_log_dict()
            log_dict["DTS"] = str(datetime.now(timezone.utc))
            log_dict["process"] = "peer_want_capture_main-1"
            log_dict["pid"] = pid
            log_dict["peer_type"] = peer_type
            log_dict["msg"] = msg
            if peers_processed > 0:
                insert_log_row(conn, queries, log_dict)
                conn.commit()

            interval_count += 1
            try:
                msg = peer_queue.get(
                    timeout=wait_for_new_peer
                )  # comes from peer capture process
                # logger.debug(msg)

                log_dict = refresh_log_dict()
                log_dict["DTS"] = str(datetime.now(timezone.utc))
                log_dict["process"] = "peer_want_capture_main-2"
                log_dict["pid"] = pid
                log_dict["peer_type"] = peer_type
                log_dict["msg"] = msg
                # insert_log_row(conn, queries, log_dict)
                # conn.commit()
            except Empty:
                # logger.debug("Queue empty")
                msg = "Queue empty"
                log_dict = refresh_log_dict()
                log_dict["DTS"] = str(datetime.now(timezone.utc))
                log_dict["process"] = "peer_want_capture_main-3"
                log_dict["pid"] = pid
                log_dict["peer_type"] = peer_type
                log_dict["msg"] = msg
                insert_log_row(conn, queries, log_dict)
                conn.commit()

            except AttributeError:
                sleep(60)
            interval_count += 1
            current_DT = datetime.now()

        log_string = f"{total_peers_processed} {peer_type} peers submitted for Want List  processing."
        # logger.info(log_string)
        msg = log_string
        log_dict = refresh_log_dict()
        log_dict["DTS"] = str(datetime.now(timezone.utc))
        log_dict["process"] = "peer_want_capture_main-0"
        log_dict["pid"] = pid
        log_dict["peer_type"] = peer_type
        log_dict["msg"] = msg
        insert_log_row(conn, queries, log_dict)
        conn.commit()
        log_string = "Normal shutdown of Want List Capture."
        msg = log_string
        log_dict = refresh_log_dict()
        log_dict["DTS"] = str(datetime.now(timezone.utc))
        log_dict["process"] = "peer_want_capture_main-0"
        log_dict["pid"] = pid
        log_dict["peer_type"] = peer_type
        log_dict["msg"] = msg
        insert_log_row(conn, queries, log_dict)
        conn.commit()
        conn.close()
    return


def capture_want_lists_for_peers(
    # logger,
    want_list_config_dict,
    peer_type,
    pool,
):
    peers_processed = 0
    p = psutil.Process()
    pid = p.pid
    DTS = get_DTS()
    connR, queries = set_up_sql_operations(want_list_config_dict)
    connU, queries = set_up_sql_operations(want_list_config_dict)
    # dual connections avoid locking conflict with the read

    found = True
    while found:
        row_for_peer = queries.select_peers_by_peer_type_status(
            connR, peer_type=peer_type
        )
        while row_for_peer:
            peer_table_dict = refresh_peer_table_dict()
            peer_table_dict["peer_ID"] = row_for_peer["peer_ID"]
            peer_table_dict["peer_type"] = row_for_peer["peer_type"]
            peer_table_dict["processing_status"] = (
                "WLP"  # suppress resubmission by WLR -> WLP
            )
            peer_table_dict["local_update_DTS"] = DTS

            update_peer_table_status_WLP(connU, queries, peer_table_dict)
            connU.commit()
            pool.apply_async(
                submitted_capture_peer_want_list_by_id,
                args=(
                    want_list_config_dict,
                    peer_table_dict,
                ),
            )

            # logger.debug(f"peer {peers_processed} id {peer_table_dict['peer_ID']}.")
            msg = f"peer {peers_processed} id {peer_table_dict['peer_ID']}."
            log_dict = refresh_log_dict()
            log_dict["DTS"] = str(datetime.now(timezone.utc))
            log_dict["process"] = "peer_want_capture_peer-1"
            log_dict["pid"] = pid
            log_dict["peer_type"] = peer_type
            log_dict["msg"] = msg
            insert_log_row(connU, queries, log_dict)
            connU.commit()
            peers_processed += 1
            row_for_peer = queries.select_peers_by_peer_type_status(
                connR, peer_type=peer_type
            )

        found = False

    connR.close()
    connU.close()

    return peers_processed


def submitted_capture_peer_want_list_by_id(
    want_list_config_dict,
    peer_table_dict,
):
    p = psutil.Process()
    p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # NOTE: put in config
    pid = p.pid
    peer_type = peer_table_dict["peer_type"]
    peer_ID = peer_table_dict["peer_ID"]
    conn, queries = set_up_sql_operations(want_list_config_dict)
    logger = get_logger_task(peer_type, peer_ID)

    msg = f"Want list capture for {peer_ID}, pid {pid}, and type {peer_type} started."
    log_dict = refresh_log_dict()
    log_dict["DTS"] = str(datetime.now(timezone.utc))
    log_dict["process"] = "peer_want_capture_task-1"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = msg
    insert_log_row(conn, queries, log_dict)
    conn.commit()

    url_dict = get_url_dict()
    DTS = get_DTS()
    peer_table_dict["processing_status"] = "WLX"
    peer_table_dict["local_update_DTS"] = DTS
    # indicate processing is active for this peer WLP -> WLX
    update_peer_table_status_WLX(conn, queries, peer_table_dict)
    conn.commit()
    # conn.close

    queue_server = BaseManager(address=("127.0.0.1", 50000), authkey=b"abc")
    if peer_type == "PP":
        queue_server.register("get_provider_queue")
        queue_server.connect()
        peer_queue = queue_server.get_provider_queue()
        max_zero_sample_count = int(want_list_config_dict["provider_zero_sample_count"])

        param = {"arg": peer_ID}

        response, status_code = execute_request(
            url_key="find_peer",
            logger=logger,
            url_dict=url_dict,
            config_dict=want_list_config_dict,
            param=param,
        )

        if status_code == 200:
            peer_ip_address_list = decode_findpeer_structure(response)
            for peer_ip_address in peer_ip_address_list:
                param = {"arg": peer_ip_address + "/p2p/" + peer_ID}

                log_string = f"{peer_ip_address}"
                msg = log_string
                log_dict = refresh_log_dict()
                log_dict["DTS"] = str(datetime.now(timezone.utc))
                log_dict["process"] = "peer_want_capture_task-peer-address"
                log_dict["pid"] = pid
                log_dict["peer_type"] = peer_type
                log_dict["msg"] = msg
                insert_log_row(conn, queries, log_dict)
                conn.commit()

        """

        #param = {"arg": peer_address + "/p2p/" + responses_dict["ID"]}
        response, status_code = execute_request(
            url_key="connect",
            logger=logger,
            url_dict=url_dict,
            config_dict=want_list_config_dict,
            param=param,
        )
        response, status_code = execute_request(
            url_key="peering_add",
            logger=logger,
            url_dict=url_dict,
            config_dict=want_list_config_dict,
            param=param,
        )
        """
        # peer_table_dict["processing_status"] = "WLR"

    elif peer_type == "BP":
        queue_server.register("get_bitswap_queue")
        queue_server.connect()
        peer_queue = queue_server.get_bitswap_queue()
        max_zero_sample_count = int(want_list_config_dict["bitswap_zero_sample_count"])
        # peer_table_dict["processing_status"] = "WLR"

    elif peer_type == "SP":
        queue_server.register("get_swarm_queue")
        queue_server.connect()
        peer_queue = queue_server.get_swarm_queue()
        max_zero_sample_count = int(want_list_config_dict["bitswap_zero_sample_count"])
        # peer_table_dict["processing_status"] = "WLR"

    # this is one sample interval for one peer
    number_of_samples_per_interval = int(
        want_list_config_dict["number_of_samples_per_interval"]
    )  # per_interval
    seconds_per_sample = 60 // int(want_list_config_dict["samples_per_minute"])
    wait_seconds = seconds_per_sample
    samples = 0
    zero_sample_count = 0
    found = 0
    added = 0
    updated = 0
    total_found = 0
    total_added = 0
    total_updated = 0
    NCW_count = 0

    # conn, queries = set_up_sql_operations(want_list_config_dict)
    while (
        samples < number_of_samples_per_interval
        # NOTE: will this condition allow the wlz to be processed twice?
        and zero_sample_count <= max_zero_sample_count
        # provider peers have the threshold set to 1440 to provide an infinite processing cycle
    ):
        sleep(wait_seconds)
        found, added, updated = capture_peer_want_list_by_id(
            logger, want_list_config_dict, peer_table_dict
        )
        total_found += found
        total_added += added
        total_updated += updated

        if found == 0:
            zero_sample_count += 1
        else:
            zero_sample_count -= 1

        if (
            zero_sample_count == max_zero_sample_count
        ):  # sampling permanently completed due to no want list available for peer
            peer_table_dict["processing_status"] = "WLZ"
            peer_table_dict["local_update_DTS"] = DTS
            update_peer_table_status_WLZ(conn, queries, peer_table_dict)
            conn.commit()
            # conn.close
            NCW_count += 1  # BUG: how does this get to 2?

        samples += 1

    # TODO: if pp test for capture Here each interval
    if peer_type == "PP":
        filter_wantlist(pid, conn)

    if zero_sample_count < max_zero_sample_count:  # sampling interval completed
        # conn, queries = set_up_sql_operations(
        #    want_list_config_dict
        # )  # set from WLX to WLR so sampling will be continued
        peer_table_dict["processing_status"] = "WLR"
        peer_table_dict["local_update_DTS"] = DTS
        update_peer_table_status_WLR(conn, queries, peer_table_dict)
        conn.commit()
        # conn.close

    log_string = f"In {samples} samples, {total_found} found, {total_added} added, {total_updated} updated and NCW {NCW_count} count for {peer_ID}"
    # logger.debug(log_string)
    """
    if peer_type == "PP":

        response, status_code = execute_request(
            url_key="dis_connect",
            logger=logger,
            url_dict=url_dict,
            config_dict=want_list_config_dict,
            param=param,
        )
        response, status_code = execute_request(
            url_key="peering_remove",
            logger=logger,
            url_dict=url_dict,
            config_dict=want_list_config_dict,
            param=param,
        )
    """

    msg = log_string
    log_dict = refresh_log_dict()
    log_dict["DTS"] = str(datetime.now(timezone.utc))
    log_dict["process"] = "peer_want_capture_task-2"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = msg
    insert_log_row(conn, queries, log_dict)
    conn.commit()

    log_string = (
        f"Want list capture for {peer_ID}, pid {pid}, and type {peer_type} completed."
    )
    peer_queue.put_nowait(
        f"Want list capture for {peer_ID}, pid {pid}, and type {peer_type} completed."
    )

    msg = log_string
    log_dict = refresh_log_dict()
    log_dict["DTS"] = str(datetime.now(timezone.utc))
    log_dict["process"] = "peer_want_capture_task-3"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = msg
    insert_log_row(conn, queries, log_dict)
    conn.commit()
    conn.close()
    return


def capture_peer_want_list_by_id(
    logger,
    want_list_config_dict,
    peer_table_dict,
):  # This is one sample for a peer
    url_dict = get_url_dict()

    found = 0
    added = 0
    updated = 0

    param = {"peer": peer_table_dict["peer_ID"]}
    response, status_code = execute_request(
        url_key="want_list",
        logger=logger,
        url_dict=url_dict,
        config_dict=want_list_config_dict,
        param=param,
    )

    level_zero_dict = json.loads(response.text)
    found, added, updated = decode_want_list_structure(
        want_list_config_dict, peer_table_dict, level_zero_dict
    )

    return found, added, updated


def decode_want_list_structure(want_list_config_dict, peer_table_dict, level_zero_dict):
    conn, queries = set_up_sql_operations(want_list_config_dict)
    found = 0
    added = 0
    updated = 0
    level_one_list = level_zero_dict["Keys"]
    if str(level_one_list) != "None":
        for level_two_dict in level_one_list:
            want_item = level_two_dict["/"]
            DTS = get_DTS()
            want_list_table_dict = refresh_want_list_table_dict()
            want_list_table_dict["peer_ID"] = peer_table_dict["peer_ID"]
            want_list_table_dict["object_CID"] = want_item
            want_list_table_dict["insert_DTS"] = DTS
            want_list_table_dict["source_peer_type"] = peer_table_dict["peer_type"]

            try:
                insert_want_list_row(conn, queries, want_list_table_dict)
                conn.commit()
                added += 1
            except IntegrityError:  # assumed to be dup key error
                want_list_entry = select_want_list_entry_by_key(
                    conn, queries, want_list_table_dict
                )
                want_list_table_dict["last_update_DTS"] = DTS
                insert_dt = datetime.fromisoformat(want_list_entry["insert_DTS"])
                update_dt = datetime.fromisoformat(
                    want_list_table_dict["last_update_DTS"]
                )
                delta = update_dt - insert_dt
                want_list_table_dict["insert_update_delta"] = int(delta.total_seconds())

                update_last_update_DTS(conn, queries, want_list_table_dict)
                conn.commit()
                updated += 1

            found += 1

    conn.close()
    return found, added, updated


def decode_findpeer_structure(
    response,
):
    # json_dict = json.loads(response.text)
    # print(json_dict)
    peer_ip_address_list = {"null"}
    for line in response.iter_lines():
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
                peer_ip_address_list = responses_dict["Addrs"]

    return peer_ip_address_list


def filter_wantlist(pid, conn):
    want_list_config_dict = get_want_list_config_dict()
    # path_dict = get_path_dict()

    conn, queries = set_up_sql_operations(want_list_config_dict)

    current_DT = datetime.now(timezone.utc)
    off_set = timedelta(hours=1)
    duration = timedelta(hours=1)
    start_dts = current_DT - off_set
    end_dts = start_dts + duration
    query_start_dts = datetime.isoformat(start_dts)
    query_stop_dts = datetime.isoformat(end_dts)

    # print(query_start_dts)
    # print(query_stop_dts)
    rows_of_wantlist_items = queries.select_filter_want_list_by_start_stop(
        conn,
        query_start_dts=query_start_dts,
        query_stop_dts=query_stop_dts,
    )

    for want_list_item in rows_of_wantlist_items:
        # print(f"Object CID: {want_list_item['object_CID']}")

        log_string = f"Object {want_list_item['object_CID']} found."
        msg = log_string
        log_dict = refresh_log_dict()
        log_dict["DTS"] = str(datetime.now(timezone.utc))
        log_dict["process"] = "wantlist-filter-1"
        log_dict["pid"] = pid
        log_dict["peer_type"] = "PP"
        log_dict["msg"] = msg
        insert_log_row(conn, queries, log_dict)
        conn.commit()

        # IPNS_name = want_list_item["object_CID"]
        # back_slash = "\\"
        # dot_txt = ".txt"
        # out_path = str(path_dict['log_path']) + back_slash + IPNS_name + dot_txt
        # out_file = open(out_path, 'wb')
        # print(out_path)
        url_dict = get_url_dict()
        param = {
            "arg": want_list_item["object_CID"],
        }
        url_key = "get"
        # config_dict = want_list_config_dict
        # file = "none"
        response, status_code = execute_request(  # BUG: error handling
            url_key,
            url_dict=url_dict,
            config_dict=want_list_config_dict,
            param=param,
            timeout=(3.05, 27),
        )
        if status_code == 200:
            # print(response.headers["X-Content-Length"])
            X_Content_Length = int(response.headers["X-Content-Length"])

            if X_Content_Length >= 130 and X_Content_Length <= 170:
                start = response.text.find("{")
                # print(start)
                if start > 0:
                    end = response.text.find("}", start)
                    if end > 0:
                        end += 1
                        string = response.text[start:end]
                        try:
                            json_dict = json.loads(string)
                            try:
                                IPNS_name = json_dict["IPNS_name"]
                            except KeyError:
                                break
                        except json.JSONDecodeError:
                            break

                        # print(f"IPNS_name: {json_dict['IPNS_name']}")
                        # print(json_dict["IPNS_name"])

                        DTS = get_DTS()
                        peer_table_dict = refresh_peer_table_dict()
                        # Read here for current status
                        # Uconn, queries = set_up_sql_operations(want_list_config_dict)
                        peer_table_dict["IPNS_name"] = IPNS_name
                        peer_table_dict["processing_status"] = "NPC"
                        peer_table_dict["local_update_DTS"] = DTS
                        peer_table_dict["peer_ID"] = want_list_item["peer_ID"]

                        # update_peer_table_IPNS_name_status_NPC(Uconn, queries, peer_table_dict)
                        # Uconn.commit()
                        # Uconn.close
                        # print("Success")
                        log_string = f"IPNS name {IPNS_name} found  in {want_list_item['object_CID']}."
                        msg = log_string
                        log_dict = refresh_log_dict()
                        log_dict["DTS"] = str(datetime.now(timezone.utc))
                        log_dict["process"] = "wantlist-filter-2"
                        log_dict["pid"] = pid
                        log_dict["peer_type"] = "PP"
                        log_dict["msg"] = msg
                        insert_log_row(conn, queries, log_dict)
                        conn.commit()

                        """
                        param = {"arg": peer_table_dict["peer_ID"]}
                        execute_request(
                            url_key="find_peer",
                            logger=logger,
                            url_dict=url_dict,
                            config_dict=capture_peer_config_dict,
                            param=param,
                        )

                        status, peer_address = decode_find_peer_structure(
                            response,
                        )

                        if status:

                            param = {"arg": peer_address + "/p2p/" + responses_dict["ID"]}
                            execute_request(
                                url_key="peering_rm",
                                logger=logger,
                                url_dict=url_dict,
                                config_dict=capture_peer_config_dict,
                                param=param,
                            )
                            execute_request(
                                url_key="disconnect",
                                logger=logger,
                                url_dict=url_dict,
                                config_dict=capture_peer_config_dict,
                                param=param,
                            )"""

    conn.close()
    return


if __name__ == "__main__":
    freeze_support()
    set_start_method("spawn")
    capture_peer_want_lists("PP")
