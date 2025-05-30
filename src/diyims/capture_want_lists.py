# import json
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
    refresh_peer_row_from_template,
    refresh_want_list_table_dict,
    set_up_sql_operations,
    update_peer_table_status_WLR,
    update_peer_table_status_WLP,
    update_peer_table_status_WLX,
    update_peer_table_status_WLZ,
    update_peer_table_status_to_NPC,
    refresh_log_dict,
    insert_log_row,
    # select_peer_table_entry_by_key,
)
from diyims.general_utils import get_DTS, get_shutdown_target
from diyims.ipfs_utils import get_url_dict
from diyims.logger_utils import get_logger_task, get_logger
from diyims.config_utils import get_want_list_config_dict
from diyims.path_utils import get_path_dict


def capture_peer_want_lists(peer_type):  # each peer type runs in its own process
    freeze_support()
    try:
        set_start_method("spawn")
    except RuntimeError:
        pass
    p = psutil.Process()

    pid = p.pid

    want_list_config_dict = get_want_list_config_dict()
    logger = get_logger(
        want_list_config_dict["log_file"],
        "none",
    )
    conn, queries = set_up_sql_operations(want_list_config_dict)  # + 1
    wait_seconds = int(want_list_config_dict["wait_before_startup"])

    log_string = f"Waiting for {wait_seconds} seconds before startup."

    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "peer_want_capture_main-0"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = log_string
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()

    sleep(wait_seconds)
    log_string = "Startup of Want List Capture."

    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()

    log_dict["process"] = "peer_want_capture_main-0"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = log_string
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()

    target_DT = get_shutdown_target(want_list_config_dict)

    max_intervals = int(want_list_config_dict["max_intervals"])
    number_of_samples_per_interval = int(
        want_list_config_dict["number_of_samples_per_interval"]
    )
    seconds_per_sample = 60 // int(want_list_config_dict["samples_per_minute"])
    total_seconds = number_of_samples_per_interval * seconds_per_sample
    wait_for_new_peer = 60 * int(want_list_config_dict["wait_for_new_peer_minutes"])
    log_string = f"Shutdown target {target_DT} or {max_intervals} intervals of {total_seconds} seconds."

    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "peer_want_capture_main-0"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = log_string
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()

    interval_count = 0
    total_peers_processed = 0
    q_server_port = int(want_list_config_dict["q_server_port"])
    queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
    if peer_type == "PP":
        p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # NOTE: put in config

        queue_server.register("get_provider_queue")
        queue_server.connect()
        peer_queue = queue_server.get_provider_queue()
        pool_workers = int(want_list_config_dict["provider_pool_workers"])
        maxtasks = int(want_list_config_dict["provider_maxtasks"])
    elif peer_type == "BP":
        p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # NOTE: put in config

        queue_server.register("get_bitswap_queue")
        queue_server.connect()
        peer_queue = queue_server.get_bitswap_queue()
        pool_workers = int(want_list_config_dict["bitswap_pool_workers"])
        maxtasks = int(want_list_config_dict["bitswap_maxtasks"])
    elif peer_type == "SP":
        p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # NOTE: put in config

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

            log_string = "peer selection  for Want List processing."

            log_dict = refresh_log_dict()
            log_dict["DTS"] = get_DTS()
            log_dict["process"] = "peer_want_capture_main-1A"
            log_dict["pid"] = pid
            log_dict["peer_type"] = peer_type
            log_dict["msg"] = log_string
            # if peers_processed > 0:
            # insert_log_row(conn, queries, log_dict)
            # conn.commit()

            peers_processed = capture_want_lists_for_peers(
                want_list_config_dict,
                peer_type,
                pool,
                conn,
                queries,
                logger,
            )
            # conn, queries = set_up_sql_operations(want_list_config_dict)  # + 1
            total_peers_processed += peers_processed
            log_string = f"{peers_processed} {peer_type} peers submitted for Want List processing."

            log_dict = refresh_log_dict()
            log_dict["DTS"] = get_DTS()
            log_dict["process"] = "peer_want_capture_main-1"
            log_dict["pid"] = pid
            log_dict["peer_type"] = peer_type
            log_dict["msg"] = log_string
            insert_log_row(conn, queries, log_dict)
            conn.commit()

            interval_count += 1
            try:
                log_string = peer_queue.get(
                    timeout=wait_for_new_peer
                )  # comes from peer capture process

                log_dict = refresh_log_dict()
                log_dict["DTS"] = get_DTS()
                log_dict["process"] = "peer_want_capture_main-2"
                log_dict["pid"] = pid
                log_dict["peer_type"] = peer_type
                log_dict["msg"] = log_string
                # insert_log_row(conn, queries, log_dict)
                # conn.commit()
            except Empty:
                log_string = "Queue empty"
                log_dict = refresh_log_dict()
                log_dict["DTS"] = get_DTS()
                log_dict["process"] = "peer_want_capture_main-3"
                log_dict["pid"] = pid
                log_dict["peer_type"] = peer_type
                log_dict["msg"] = log_string
                # insert_log_row(conn, queries, log_dict)
                # conn.commit()

            except AttributeError:
                sleep(60)
            interval_count += 1
            current_DT = datetime.now()

        # conn, queries = set_up_sql_operations(want_list_config_dict)  # + 1
        log_string = f"{total_peers_processed} {peer_type} total peers submitted for Want List  processing."

        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "peer_want_capture_main-0A"
        log_dict["pid"] = pid
        log_dict["peer_type"] = peer_type
        log_dict["msg"] = log_string
        insert_log_row(conn, queries, log_dict)
        conn.commit()

        log_string = "Normal shutdown of Want List Capture."
        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "peer_want_capture_main-0"
        log_dict["pid"] = pid
        log_dict["peer_type"] = peer_type
        log_dict["msg"] = log_string
        insert_log_row(conn, queries, log_dict)
        conn.commit()
    conn.close()  # -1
    return


def capture_want_lists_for_peers(
    want_list_config_dict, peer_type, pool, conn, queries, logger
):
    from diyims.ipfs_utils import get_url_dict

    peers_processed = 0
    p = psutil.Process()
    pid = p.pid
    url_dict = get_url_dict()
    Rconn, Rqueries = set_up_sql_operations(want_list_config_dict)  # + 1
    # Uconn, Uqueries = set_up_sql_operations(want_list_config_dict)
    # conn, queries = set_up_sql_operations(want_list_config_dict)  # + 1
    # dual connections avoid locking conflict with the read

    rows_of_peers = Rqueries.select_peers_by_peer_type_status(  # NOTE: where status is WLR set by peer capture or peer not finished
        Rconn, peer_type=peer_type
    )

    for peer_row in rows_of_peers:
        param = {"arg": peer_row["version"]}

        response, status_code, response_dict = execute_request(
            url_key="connect",
            logger=logger,
            url_dict=url_dict,
            config_dict=want_list_config_dict,
            param=param,
        )

        peer_table_dict = refresh_peer_row_from_template()
        peer_table_dict["peer_ID"] = peer_row["peer_ID"]
        peer_table_dict["peer_type"] = peer_row["peer_type"]
        peer_table_dict["processing_status"] = (
            "WLP"  # suppress resubmission by WLR -> WLP
        )
        peer_table_dict["local_update_DTS"] = get_DTS()

        update_peer_table_status_WLP(conn, queries, peer_table_dict)
        conn.commit()

        pool.apply_async(
            submitted_capture_peer_want_list_by_id,
            args=(
                want_list_config_dict,
                peer_table_dict,
            ),
        )

        log_string = f"peer {peers_processed} id {peer_table_dict['peer_ID']}."
        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "peer_want_capture_peer-1"
        log_dict["pid"] = pid
        log_dict["peer_type"] = peer_type
        log_dict["msg"] = log_string
        # insert_log_row(conn, queries, log_dict)
        # conn.commit()
        peers_processed += 1

    Rconn.close()  # - 1

    return peers_processed


def submitted_capture_peer_want_list_by_id(
    want_list_config_dict,
    peer_table_dict,
):
    p = psutil.Process()
    path_dict = get_path_dict()
    pid = p.pid
    peer_type = peer_table_dict["peer_type"]
    peer_ID = peer_table_dict["peer_ID"]
    conn, queries = set_up_sql_operations(want_list_config_dict)  # + 1
    # Uconn, Uqueries = set_up_sql_operations(want_list_config_dict)
    Rconn, Rqueries = set_up_sql_operations(want_list_config_dict)
    url_dict = get_url_dict()
    logger = get_logger(
        want_list_config_dict["log_file"],
        "none",
    )

    response, status_code, response_dict = execute_request(
        url_key="id",
        logger=logger,
        url_dict=url_dict,
        config_dict=want_list_config_dict,
    )

    self = response_dict["ID"]

    logger = get_logger_task(peer_type, peer_ID)

    log_string = (
        f"Want list capture for {peer_ID}, pid {pid}, and type {peer_type} started."
    )
    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "peerID_want_list_capture_task-1"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = log_string
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()

    # url_dict = get_url_dict()

    peer_table_dict["processing_status"] = "WLX"
    peer_table_dict["local_update_DTS"] = get_DTS()

    # indicate processing is active for this peer WLP -> WLX
    update_peer_table_status_WLX(
        conn, queries, peer_table_dict
    )  # NOTE: This is where processing actually begins
    conn.commit()
    # Uconn.close

    queue_server = BaseManager(address=("127.0.0.1", 50000), authkey=b"abc")
    # queue_server.register("get_peer_maint_queue")
    # queue_server.connect()
    # peer_maint_queue = queue_server.get_peer_maint_queue()
    if peer_type == "PP":
        p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # NOTE: put in config

        queue_server.register("get_provider_queue")
        queue_server.connect()
        peer_queue = queue_server.get_provider_queue()
        max_zero_sample_count = int(want_list_config_dict["provider_zero_sample_count"])

        # peer_table_dict["processing_status"] = "WLR"

    elif peer_type == "BP":
        p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # NOTE: put in config

        queue_server.register("get_bitswap_queue")
        queue_server.connect()
        peer_queue = queue_server.get_bitswap_queue()
        max_zero_sample_count = int(want_list_config_dict["bitswap_zero_sample_count"])
        # peer_table_dict["processing_status"] = "WLR"

    elif peer_type == "SP":
        p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)  # NOTE: put in config

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
        and zero_sample_count <= max_zero_sample_count
        # provider peers have the threshold set to 9999 to provide an infinite processing cycle
    ):
        # TODO: test for already completed peer since the last sample may have pu dated the peer to NPC.
        # this is to avoid consuming IPFS resources unnecessarily.

        sleep(wait_seconds)

        found, added, updated = capture_peer_want_list_by_id(
            logger,
            want_list_config_dict,
            peer_table_dict,
            conn,
            queries,
            pid,
            peer_type,
            Rconn,
            Rqueries,
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
            # Uconn, Uqueries = set_up_sql_operations(want_list_config_dict)
            peer_table_dict["processing_status"] = "WLZ"
            peer_table_dict["local_update_DTS"] = get_DTS()
            update_peer_table_status_WLZ(conn, queries, peer_table_dict)
            conn.commit()
            NCW_count += 1

        samples += 1

        # conn, queries = set_up_sql_operations(want_list_config_dict)
        log_string = (
            f"another sample in {samples} samples for {peer_ID} of type {peer_type}."
        )
        # logger.debug(log_string)

        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "peerID_want_list_capture_task-2"
        log_dict["pid"] = pid
        log_dict["peer_type"] = peer_type
        log_dict["msg"] = log_string
        # insert_log_row(conn, queries, log_dict)
        # conn.commit()

        # if pp test for capture Here each sample
        # it is better to use db resources rather than IPFS
        if peer_type == "PP":
            peer_verified = filter_wantlist(
                pid,
                logger,
                want_list_config_dict,
                conn,
                queries,
                path_dict,
                Rconn,
                Rqueries,
                peer_ID,
                self,
            )
            if peer_verified:
                break

    if zero_sample_count < max_zero_sample_count:  # sampling interval completed
        # set from WLX to WLR so sampling will be continued if not NPC

        peer_table_dict["processing_status"] = "WLR"
        peer_table_dict["local_update_DTS"] = get_DTS()
        update_peer_table_status_WLR(conn, queries, peer_table_dict)  # logic in sql
        conn.commit()

    log_string = f"In {samples} samples, {total_found} found, {total_added} added, {total_updated} updated and NCW {NCW_count} count for {peer_ID}"
    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "peer_want_list_capture_task-3"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = log_string
    insert_log_row(conn, queries, log_dict)
    conn.commit()

    log_string = (
        f"Want list capture for {peer_ID}, pid {pid}, and type {peer_type} completed."
    )
    peer_queue.put_nowait(
        f"Want list capture for {peer_ID}, pid {pid}, and type {peer_type} completed."
    )

    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "peer_want_list_capture_task-4"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = log_string
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()
    conn.close()  # - 1
    Rconn.close()
    return


def capture_peer_want_list_by_id(
    logger,
    want_list_config_dict,
    peer_table_dict,
    conn,
    queries,
    pid,
    peer_type,
    Rconn,
    Rqueries,
):  # This is one sample for a peer
    url_dict = get_url_dict()

    found = 0
    added = 0
    updated = 0

    peer_ID = peer_table_dict["peer_ID"]
    param = {"peer": peer_table_dict["peer_ID"]}
    response, status_code, response_dict = execute_request(
        url_key="want_list",
        logger=logger,
        url_dict=url_dict,
        config_dict=want_list_config_dict,
        param=param,
    )
    log_string = f"Want list capture for {peer_ID} results {response_dict} completed with {status_code}."
    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "peer_want_list_capture_task-4"
    log_dict["pid"] = pid
    log_dict["peer_type"] = peer_type
    log_dict["msg"] = log_string
    insert_log_row(conn, queries, log_dict)
    conn.commit()

    # level_zero_dict = json.loads(response.text)
    if str(response_dict["Keys"]) == "None":
        return found, added, updated

    else:
        found, added, updated = decode_want_list_structure(
            # want_list_config_dict,
            peer_table_dict,
            response_dict,
            conn,
            queries,
            Rconn,
            Rqueries,
        )

    return found, added, updated


def decode_want_list_structure(
    # want_list_config_dict,
    peer_table_dict,
    response_dict,
    conn,
    queries,
    Rconn,
    Rqueries,
):
    found = 0
    added = 0
    updated = 0
    p = psutil.Process()

    pid = p.pid
    level_one_list = response_dict["Keys"]
    # if str(level_one_list) != "None":
    for level_two_dict in level_one_list:
        want_item = level_two_dict["/"]
        peer_ID = peer_table_dict["peer_ID"]

        want_list_table_dict = refresh_want_list_table_dict()  # TODO: rename template
        want_list_table_dict["peer_ID"] = peer_table_dict["peer_ID"]
        want_list_table_dict["object_CID"] = want_item
        want_list_table_dict["insert_DTS"] = get_DTS()
        want_list_table_dict["source_peer_type"] = peer_table_dict["peer_type"]
        peer_type = peer_table_dict["peer_type"]

        try:
            insert_want_list_row(conn, queries, want_list_table_dict)
            conn.commit()
            added += 1
            log_string = f"new want item row for peer {peer_ID}"

            log_dict = refresh_log_dict()
            log_dict["DTS"] = get_DTS()
            log_dict["process"] = "peer_want_capture_decode-1"
            log_dict["pid"] = pid
            log_dict["peer_type"] = peer_type
            log_dict["msg"] = log_string
            # insert_log_row(conn, queries, log_dict)
            # conn.commit()
        except IntegrityError:  # assumed to be dup key error
            want_list_entry = select_want_list_entry_by_key(
                Rconn, Rqueries, want_list_table_dict
            )
            want_list_table_dict["last_update_DTS"] = get_DTS()
            insert_dt = datetime.fromisoformat(want_list_entry["insert_DTS"])
            update_dt = datetime.fromisoformat(want_list_table_dict["last_update_DTS"])
            delta = update_dt - insert_dt
            want_list_table_dict["insert_update_delta"] = int(delta.total_seconds())

            update_last_update_DTS(conn, queries, want_list_table_dict)
            conn.commit()
            updated += 1

            log_string = f"update want item row for {peer_ID}"

            log_dict = refresh_log_dict()
            log_dict["DTS"] = get_DTS()
            log_dict["process"] = "peer_want_capture_decode-2"
            log_dict["pid"] = pid
            log_dict["peer_type"] = peer_type
            log_dict["msg"] = log_string
            # insert_log_row(conn, queries, log_dict)
            # conn.commit()

        found += 1

    return found, added, updated


def filter_wantlist(
    pid,
    logger,
    config_dict,
    conn,
    queries,
    path_dict,
    Rconn,
    Rqueries,
    peer_ID,
    self,
):
    """
    doc string
    """

    current_DT = datetime.now(timezone.utc)
    start_off_set = timedelta(hours=1)
    window_duration = timedelta(hours=1)
    start_dts = current_DT - start_off_set
    end_dts = start_dts + window_duration
    query_start_dts = datetime.isoformat(start_dts)
    query_stop_dts = datetime.isoformat(end_dts)
    largest_delta = 295
    smallest_delta = 240
    x_content_min = 130
    x_content_max = 170  # TODO: config

    rows_of_wantlist_items = Rqueries.select_filter_want_list_by_start_stop(
        Rconn,
        query_start_dts=query_start_dts,
        query_stop_dts=query_stop_dts,
        peer_ID=peer_ID,
        largest_delta=largest_delta,
        smallest_delta=smallest_delta,
    )

    log_string = f"Filter entered for {peer_ID}"

    log_dict = refresh_log_dict()
    log_dict["DTS"] = get_DTS()
    log_dict["process"] = "wantlist-filter-N0"
    log_dict["pid"] = pid
    log_dict["peer_type"] = "PP"
    log_dict["msg"] = log_string
    # insert_log_row(conn, queries, log_dict)
    # conn.commit()

    peer_verified = 0
    for want_list_item_row in rows_of_wantlist_items:
        want_list_object_CID = want_list_item_row[
            "object_CID"
        ]  # This is the json file containing the peer row cid

        log_string = f"Object {want_list_object_CID} found between {query_start_dts} and {query_stop_dts} for {peer_ID}."

        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "wantlist-filter-N1"
        log_dict["pid"] = pid
        log_dict["peer_type"] = "PP"
        log_dict["msg"] = log_string
        # insert_log_row(conn, queries, log_dict)
        # conn.commit()

        url_dict = get_url_dict()
        param = {
            "arg": want_list_object_CID,  # json file
        }
        url_key = "cat"

        start_DTS = get_DTS()

        response, status_code, response_dict = execute_request(
            url_key,
            url_dict=url_dict,
            config_dict=config_dict,
            param=param,
            timeout=(3.05, 104),
        )

        stop_DTS = get_DTS()
        start = datetime.fromisoformat(start_DTS)
        stop = datetime.fromisoformat(stop_DTS)
        duration = stop - start

        if status_code == 200:
            X_Content_Length = int(response.headers["X-Content-Length"])

            log_string = f"CAT result {status_code} used {duration} with dictionary of {response_dict} with {X_Content_Length} for {peer_ID}."

            log_dict = refresh_log_dict()
            log_dict["DTS"] = get_DTS()
            log_dict["process"] = "wantlist-filter-N2"
            log_dict["pid"] = pid
            log_dict["peer_type"] = "PP"
            log_dict["msg"] = log_string
            insert_log_row(conn, queries, log_dict)
            conn.commit()

            if (
                X_Content_Length >= x_content_min and X_Content_Length <= x_content_max
            ):  # this will filter out most of the false positives
                peer_row_CID = extract_peer_row_CID(
                    response_dict,
                    pid,
                )

                if peer_row_CID != "null":
                    log_string = f"Verify and update for {peer_ID}."

                    log_dict = refresh_log_dict()
                    log_dict["DTS"] = get_DTS()
                    log_dict["process"] = "wantlist-filter-N3"
                    log_dict["pid"] = pid
                    log_dict["peer_type"] = "PP"
                    log_dict["msg"] = log_string
                    insert_log_row(conn, queries, log_dict)
                    conn.commit()
                    try:
                        peer_row_CID = response_dict[
                            "peer_row_CID"
                        ]  # from want item json file
                        version = 1

                    except KeyError:
                        peer_row_CID = response_dict[
                            "peer_row_CID"
                        ]  # from want item json file
                        version = 0

                    if version == 1:
                        peer_verified = verify_peer_and_update(
                            peer_row_CID,
                            logger,
                            config_dict,
                            conn,
                            queries,
                            Rconn,
                            Rqueries,
                            pid,
                            url_dict,
                            path_dict,
                            peer_ID,
                            self,
                        )

                    else:
                        log_string = "Probably early version of application dictionary."

                        log_dict = refresh_log_dict()
                        log_dict["DTS"] = get_DTS()
                        log_dict["process"] = "wantlist-filter-F5"
                        log_dict["pid"] = pid
                        log_dict["peer_type"] = "PP"
                        log_dict["msg"] = log_string
                        insert_log_row(conn, queries, log_dict)
                        conn.commit()

                else:
                    log_string = "Unknown dictionary."

                    log_dict = refresh_log_dict()
                    log_dict["DTS"] = get_DTS()
                    log_dict["process"] = "wantlist-filter-F4"
                    log_dict["pid"] = pid
                    log_dict["peer_type"] = "PP"
                    log_dict["msg"] = log_string
                    insert_log_row(conn, queries, log_dict)
                    conn.commit()
            else:
                log_string = f"{want_list_object_CID} failed header length test with{X_Content_Length}."

                log_dict = refresh_log_dict()
                log_dict["DTS"] = get_DTS()
                log_dict["process"] = "wantlist-filter-F2"
                log_dict["pid"] = pid
                log_dict["peer_type"] = "PP"
                log_dict["msg"] = log_string
                insert_log_row(conn, queries, log_dict)
                conn.commit()

        else:
            log_string = f"CAT failed for {want_list_object_CID} with {status_code}."

            log_dict = refresh_log_dict()
            log_dict["DTS"] = get_DTS()
            log_dict["process"] = "wantlist-filter-F1"
            log_dict["pid"] = pid
            log_dict["peer_type"] = "PP"
            log_dict["msg"] = log_string
            insert_log_row(conn, queries, log_dict)
            conn.commit()

        if peer_verified:
            break

    return peer_verified


def extract_peer_row_CID(
    response_dict,
    pid,
):
    try:
        peer_row_CID = response_dict["peer_row_CID"]  # from want item json file
    except KeyError:
        log_string = "dictionary did not contain a peer_row_CID."

        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "wantlist-filter-F3"
        log_dict["pid"] = pid
        log_dict["peer_type"] = "PP"
        log_dict["msg"] = log_string

        peer_row_CID = "null"

    return peer_row_CID


def verify_peer_and_update(
    peer_row_CID,
    logger,
    config_dict,
    conn,
    queries,
    Rconn,
    Rqueries,
    pid,
    url_dict,
    path_dict,
    peer_ID,
    self,
):
    from diyims.security_utils import verify_peer_row_from_cid
    from diyims.ipfs_utils import export_peer_table
    from diyims.header_utils import ipfs_header_add
    from diyims.database_utils import (
        refresh_peer_row_from_template,
    )  # , select_peer_table_entry_by_key

    peer_row_dict = refresh_peer_row_from_template()
    peer_row_dict["peer_ID"] = peer_ID

    peer_verified, peer_row_dict = verify_peer_row_from_cid(
        peer_row_CID, logger, config_dict
    )

    if peer_verified:
        peer_row_dict["local_update_DTS"] = get_DTS()

        update_peer_table_status_to_NPC(conn, queries, peer_row_dict)
        conn.commit()

        log_string = f"Peer {peer_row_dict['peer_ID']}  updated."
        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "wantlist-filter-verify-peer-N1"
        log_dict["pid"] = pid
        log_dict["peer_type"] = "PP"
        log_dict["msg"] = log_string
        insert_log_row(conn, queries, log_dict)
        conn.commit()

        object_CID = export_peer_table(  # This creates the table that is used by the maint process as peers and theirs change and are published
            conn,
            queries,
            url_dict,
            path_dict,
            config_dict,
            logger,
        )

        DTS = get_DTS()
        object_type = "peer_table_entry"
        mode = "Normal"
        peer_ID = self

        ipfs_header_add(
            DTS,
            object_CID,
            object_type,
            peer_ID,
            config_dict,
            logger,
            mode,
            conn,
            queries,
        )

    else:
        log_string = f"Peer {peer_row_dict['peer_entry_CID']} signature not valid."

        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "wantlist-filter_verify_peer-F1"
        log_dict["pid"] = pid
        log_dict["peer_type"] = "PP"
        log_dict["msg"] = log_string
        insert_log_row(conn, queries, log_dict)
        conn.commit()

    return peer_verified


if __name__ == "__main__":
    freeze_support()
    set_start_method("spawn")
    capture_peer_want_lists("PP")
