import os
from datetime import datetime, timedelta, timezone
from time import sleep
from sqlite3 import IntegrityError
from sqlmodel import create_engine, Session, select, col

# from sqlalchemy.exc import NoResultFound
from multiprocessing import set_start_method, freeze_support
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
    update_peer_table_status_WLX,
    update_peer_table_status_WLZ,
    update_peer_table_status_to_NPP,
    select_peer_table_entry_by_key,
)
from diyims.general_utils import get_DTS, shutdown_query
from diyims.ipfs_utils import unpack_peer_row_from_cid
from diyims.logger_utils import add_log
from diyims.config_utils import get_want_list_config_dict
from diyims.path_utils import get_path_dict
from diyims.sqlmodels import Peer_Address, Want_List_Table
from diyims.class_imports import WantlistCaptureProcessMainArgs, SetControlsReturn


def wantlist_capture_process_main(Args: WantlistCaptureProcessMainArgs) -> None:
    if Args.set_controls_return.component_test:
        pass
    else:
        submitted_wantlist_process_for_peer(
            Args.call_stack,
            Args.want_list_config_dict,
            Args.peer_table_dict,
            Args.set_controls_return,
        )

    return


def submitted_wantlist_process_for_peer(
    call_stack: str,
    want_list_config_dict: dict,
    peer_table_dict: dict,
    SetControlsReturn: SetControlsReturn,
) -> str:
    call_stack = call_stack + ":submitted_wantlist_process_for_peer"

    peer_type = peer_table_dict["peer_type"]
    provider_peer_ID = peer_table_dict["peer_ID"]

    conn, queries = set_up_sql_operations(want_list_config_dict)  # + 1
    log_string = (
        f"Want list capture for {provider_peer_ID} and type {peer_type} started."
    )
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=log_string,
        )

    # peer_table_dict["processing_status"] = "WLX" # WLR to WLX
    peer_table_dict["local_update_DTS"] = get_DTS()

    # indicate processing is active for this peer WLP -> WLX
    conn, queries = set_up_sql_operations(want_list_config_dict)  # + 1
    update_peer_table_status_WLX(conn, queries, peer_table_dict)
    conn.commit()
    conn.close()
    q_server_port = int(want_list_config_dict["q_server_port"])

    if SetControlsReturn.queues_enabled:  # TODO: outbound = "" ??????
        queue_server = BaseManager(
            address=("127.0.0.1", q_server_port), authkey=b"abc"
        )  # TODO:config
        queue_server.register("get_want_list_queue")
        queue_server.register("get_peer_maint_queue")
        queue_server.connect()
        # peer_maint_queue = queue_server.get_peer_maint_queue()
        if peer_type == "PP":
            # queue_server.register("get_peer_maint_queue")
            # queue_server.connect()
            in_bound = queue_server.get_want_list_queue()
            # out_bound = None
            out_bound = queue_server.get_peer_maint_queue()
            max_zero_sample_count = int(
                want_list_config_dict["provider_zero_sample_count"]
            )

            # peer_table_dict["processing_status"] = "WLR"

    else:
        out_bound = None  # TODO: enhance dependent functions with keyword handling
        # peer_table_dict["processing_status"] = "WLR"

    # this is one sample interval for one peer
    max_zero_sample_count = int(want_list_config_dict["provider_zero_sample_count"])
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
    status_code = 200
    # conn, queries = set_up_sql_operations(want_list_config_dict)
    while (
        samples < number_of_samples_per_interval
        and zero_sample_count <= max_zero_sample_count
        # provider peers have the threshold set to 9999 to provide an infinite processing cycle until NPC
    ):
        if shutdown_query(call_stack):
            break
        conn, queries = set_up_sql_operations(want_list_config_dict)

        peer_row_dict = refresh_peer_row_from_template()  # start from scratch
        peer_row_dict["peer_ID"] = provider_peer_ID
        peer_row_dict["peer_type"] = peer_type
        peer_row_entry = select_peer_table_entry_by_key(conn, queries, peer_row_dict)
        conn.close()
        if peer_row_entry["processing_status"] == "WLX":
            status_code, found, added, updated = capture_peer_want_list_by_id(
                call_stack,
                want_list_config_dict,
                peer_row_dict,
                SetControlsReturn.logging_enabled,
            )
            if shutdown_query(call_stack):
                break
            if status_code != 200:
                if SetControlsReturn.logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg="Capture want list by ID failed.",
                    )
                return status_code

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
                # peer_table_dict["processing_status"] = "WLZ"
                conn, queries = set_up_sql_operations(want_list_config_dict)  # + 1
                peer_table_dict["local_update_DTS"] = get_DTS()
                update_peer_table_status_WLZ(conn, queries, peer_table_dict)
                conn.commit()
                conn.close()
                NCW_count += 1

            samples += 1

            log_string = f"another sample in {samples} samples for {provider_peer_ID} of type {peer_type}."
            if SetControlsReturn.logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=log_string,
                )
            # if pp test for capture Here each sample
            # it is better to use db resources rather than IPFS
            if shutdown_query(call_stack):
                break

            if (
                peer_type == "PP"
            ):  # TODO: reorg filter want list to separate db and processing
                status_code, peer_verified = filter_wantlist(
                    call_stack,
                    want_list_config_dict,
                    provider_peer_ID,
                    # out_bound,
                    SetControlsReturn.queues_enabled,
                    SetControlsReturn.logging_enabled,
                )
                if status_code != 200:
                    if SetControlsReturn.logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=f"filter wantlist returned with {status_code}.",
                        )
                if peer_verified:
                    break

            if SetControlsReturn.logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=f"Entry into wait to create sample length {wait_seconds}.",
                )
            try:
                if SetControlsReturn.queues_enabled:
                    log_string = in_bound.get(timeout=wait_seconds)
                    # comes from peer capture process or shutdown
                    if shutdown_query(call_stack):
                        break
                else:
                    sleep(wait_seconds)
                    if shutdown_query(call_stack):
                        break
            except Empty:
                if shutdown_query(call_stack):
                    break
        else:
            break  #  != WLX

    if zero_sample_count < max_zero_sample_count:  # sampling interval completed
        # set from WLX to WLR so sampling will be continued for PP

        # peer_table_dict["processing_status"] = "WLR"
        conn, queries = set_up_sql_operations(want_list_config_dict)  # + 1
        peer_table_dict["local_update_DTS"] = get_DTS()
        update_peer_table_status_WLR(conn, queries, peer_table_dict)
        conn.commit()
        conn.close()

    log_string = f"In {samples} samples, {total_found} found, {total_added} added, {total_updated} updated and NCW {NCW_count} count for {provider_peer_ID}"
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=log_string,
        )

    log_string = (
        f"Want list capture for {provider_peer_ID} and type {peer_type} completed."
    )
    if SetControlsReturn.queues_enabled:
        out_bound.put_nowait("wake up")
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=log_string,
        )

    return status_code


def capture_peer_want_list_by_id(
    call_stack,
    want_list_config_dict,
    peer_row_dict,
    logging_enabled,
):  # This is one sample for a peer
    call_stack = call_stack + ":capture_peer_want_list_by_id"
    found = 0
    added = 0
    updated = 0
    # status_code = 800

    provider_peer_ID = peer_row_dict["peer_ID"]
    param = {"peer": peer_row_dict["peer_ID"]}
    response, status_code, response_dict = execute_request(
        url_key="want_list",
        config_dict=want_list_config_dict,
        param=param,
        call_stack=call_stack,
        http_500_ignore=False,
    )
    if shutdown_query(call_stack):
        return status_code, found, added, updated

    # level_zero_dict = json.loads(response.text)
    if status_code == 200:
        log_string = f"Want list capture for {provider_peer_ID} results {response_dict} completed with {status_code}."
        if logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=log_string,
            )
        if str(response_dict["Keys"]) == "None":
            pass
        else:
            status_code, found, added, updated = decode_want_list_structure(
                call_stack,
                want_list_config_dict,
                peer_row_dict,
                response_dict,
                logging_enabled,
            )
            if status_code != 200:
                if logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=f"Want List decode for {provider_peer_ID} failed with {provider_peer_ID}.",
                    )
    else:
        if logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=f"Get want list for {provider_peer_ID} failed with {status_code}.",
            )
    return status_code, found, added, updated


def decode_want_list_structure(
    call_stack,
    want_list_config_dict,
    peer_row_dict,
    response_dict,
    logging_enabled,
):
    call_stack = call_stack + ":decode_want_list_structure"
    found = 0
    added = 0
    updated = 0
    status_code = 200

    level_one_list = response_dict["Keys"]
    # if str(level_one_list) != "None":
    for level_two_dict in level_one_list:
        want_item = level_two_dict["/"]
        provider_peer_ID = peer_row_dict["peer_ID"]

        want_list_table_dict = refresh_want_list_table_dict()  # TODO: rename template
        want_list_table_dict["peer_ID"] = provider_peer_ID
        want_list_table_dict["object_CID"] = want_item
        want_list_table_dict["insert_DTS"] = get_DTS()
        want_list_table_dict["source_peer_type"] = peer_row_dict["peer_type"]
        # peer_type = peer_row_dict["peer_type"]
        conn, queries = set_up_sql_operations(want_list_config_dict)
        try:
            insert_want_list_row(conn, queries, want_list_table_dict)
            conn.commit()
            conn.close()
            added += 1
            log_string = f"new want item for {provider_peer_ID}"
            if logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=log_string,
                )

        except IntegrityError:  # assumed to be dup key error
            conn.rollback()
            conn.close()
            conn, queries = set_up_sql_operations(want_list_config_dict)

            want_list_entry = select_want_list_entry_by_key(
                conn, queries, want_list_table_dict
            )
            conn.close()
            want_list_table_dict["last_update_DTS"] = get_DTS()
            insert_dt = datetime.fromisoformat(want_list_entry["insert_DTS"])
            update_dt = datetime.fromisoformat(want_list_table_dict["last_update_DTS"])
            delta = update_dt - insert_dt
            want_list_table_dict["insert_update_delta"] = int(delta.total_seconds())
            conn, queries = set_up_sql_operations(want_list_config_dict)  # + 1
            update_last_update_DTS(conn, queries, want_list_table_dict)
            conn.commit()

            updated += 1

            log_string = f"update want item for {provider_peer_ID}"
            if logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=log_string,
                )

        found += 1

    return status_code, found, added, updated


def filter_wantlist(
    call_stack,
    config_dict,
    provider_peer_ID,
    # out_bound,
    queues_enabled,
    logging_enabled,
) -> bool:
    """
    doc string
    """
    call_stack = call_stack + ":filter_wantlist"
    current_DT = datetime.now(timezone.utc)
    start_off_set = timedelta(hours=1)
    window_duration = timedelta(hours=1)
    start_dts = current_DT - start_off_set
    end_dts = start_dts + window_duration
    query_start_dts = datetime.isoformat(start_dts)
    query_stop_dts = datetime.isoformat(end_dts)
    largest_delta = int(340)
    smallest_delta = int(240)
    x_content_min = 130
    x_content_max = 170  # TODO: config
    status_code = 200

    log_string = f"Filter entered for {provider_peer_ID}"
    if logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=log_string,
        )

    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"
    engine = create_engine(db_url, echo=False, connect_args={"timeout": 120})
    with Session(engine) as session:
        statement = (
            select(Want_List_Table)
            .where(Want_List_Table.peer_ID == provider_peer_ID)
            .where(
                col(Want_List_Table.last_update_DTS) >= query_start_dts,
                col(Want_List_Table.last_update_DTS) <= query_stop_dts,
                col(Want_List_Table.insert_update_delta) <= largest_delta,
                col(Want_List_Table.insert_update_delta) >= smallest_delta,
            )
            .order_by(col(Want_List_Table.insert_update_delta).desc())
        )
        results = session.exec(statement).all()
        line_list = []
        for want_list_item in results:
            line_list.append(want_list_item)

    peer_verified = False
    item_number = -1
    for list_item in line_list:  # iteration in range rather than list item ?
        item_number += 1
        if shutdown_query(call_stack):
            break

        conn, queries = set_up_sql_operations(config_dict)

        peer_row_dict = refresh_peer_row_from_template()
        peer_row_dict["peer_ID"] = provider_peer_ID
        peer_row_entry = select_peer_table_entry_by_key(
            conn, queries, peer_row_dict
        )  # for checking only
        conn.close()

        if (
            peer_row_entry["processing_status"] == "WLX"
        ):  # check that we are still WLX status
            want_list_object_CID = line_list[item_number].object_CID
            # This is the json file containing the peer row cid

            log_string = f"Object {want_list_object_CID} found between {query_start_dts} and {query_stop_dts} for {provider_peer_ID}."
            if logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=log_string,
                )

            param = {
                "arg": want_list_object_CID,  # from beacon
            }
            start_DTS = get_DTS()

            response, status_code, response_dict = execute_request(
                url_key="cat",
                param=param,
                call_stack=call_stack,
                timeout=(3.05, 240),
                http_500_ignore=False,
            )
            if (
                status_code != 200 and status_code != 500
            ):  # TODO: expand error reporting
                if logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="Error",
                        msg=f"Want List cat failed with {status_code}.",
                    )
                break

            stop_DTS = get_DTS()
            start = datetime.fromisoformat(start_DTS)
            stop = datetime.fromisoformat(stop_DTS)
            duration = stop - start

            if status_code == 200:
                X_Content_Length = int(response.headers["X-Content-Length"])
                conn, queries = set_up_sql_operations(config_dict)  # + 1
                log_string = f"CAT result {status_code} used {duration} with dictionary of {response_dict} with {X_Content_Length} for {provider_peer_ID}."
                if logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=log_string,
                    )
                if (
                    X_Content_Length >= x_content_min
                    and X_Content_Length <= x_content_max
                ):  # this will filter out most of the false positives and provides verification of a dictionary with peer_row_cid
                    status_code, provider_peer_row_CID = extract_peer_row_CID(
                        call_stack,
                        response_dict,
                        logging_enabled,
                    )

                    if provider_peer_row_CID != "null":
                        status_code, test_dictionary = unpack_peer_row_from_cid(
                            call_stack, provider_peer_row_CID, config_dict
                        )
                        test_peer_ID = test_dictionary["peer_ID"]
                        if test_peer_ID != provider_peer_ID:
                            log_string = f"test {test_peer_ID} and {provider_peer_ID} did not match."
                        else:
                            log_string = f"Capture and update to NPP for {provider_peer_ID}."  # This happens later in the code. Bad place for the message
                        if logging_enabled:
                            add_log(
                                process=call_stack,
                                peer_type="status",
                                msg=log_string,
                            )
                        conn, queries = set_up_sql_operations(config_dict)  # + 1
                        peer_row_dict["version"] = (
                            provider_peer_row_CID  # TODO: #58 add data dedicated data element
                        )
                        peer_row_dict["local_update_DTS"] = get_DTS()
                        update_peer_table_status_to_NPP(  # NOTE: this can be overridden by a PMP from Peer monitoring. This is the first cid
                            conn, queries, peer_row_dict
                        )  # this updates the verified flag hidden in db utils
                        conn.commit()
                        conn.close()
                        peer_verified = True
                        # if queues_enabled:
                        #    out_bound.put_nowait("wake up")
                        statement = select(Peer_Address).where(
                            Peer_Address.peer_ID == provider_peer_ID,
                            Peer_Address.in_use == 1,
                        )
                        with Session(engine) as session:
                            results = session.exec(statement)
                            address = results.first()
                            provider_address = address.multiaddress

                        param = {
                            "arg": provider_peer_ID,
                        }
                        response, status_code, response_dict = execute_request(
                            url_key="peering_remove",
                            param=param,
                            call_stack=call_stack,
                        )
                        if status_code != 200 and status_code != 500:
                            if logging_enabled:
                                add_log(
                                    process=call_stack,
                                    peer_type="status",
                                    msg=f"peering remove for {provider_peer_ID} failed with {status_code}.",
                                )
                            break

                        if status_code == 200:
                            peering_removed = True

                        param = {
                            "arg": provider_address,
                        }

                        response, status_code, response_dict = execute_request(
                            url_key="dis_connect",
                            param=param,
                            call_stack=call_stack,
                        )

                        if status_code != 200 and status_code != 500:
                            if logging_enabled:
                                add_log(
                                    process=call_stack,
                                    peer_type="status",
                                    msg=f"dis connect failed for {provider_peer_ID} with {status_code}.",
                                )
                            break

                        if status_code == 200:
                            disconnected = True

                        with Session(engine) as session:
                            results = session.exec(statement)
                            address = results.first()
                            address.in_use = False
                            if peering_removed:
                                address.peering_remove_DTS = get_DTS()
                            if disconnected:
                                address.dis_connect_DTS = get_DTS()
                            session.add(address)
                            session.commit()

                    else:
                        log_string = f"Unknown dictionary for {provider_peer_row_CID}."
                        if logging_enabled:
                            add_log(
                                process=call_stack,
                                peer_type="status",
                                msg=log_string,
                            )

                else:
                    log_string = f"{want_list_object_CID} failed header length test with{X_Content_Length}."
                    if logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=log_string,
                        )
            else:
                log_string = (
                    f"CAT failed for {want_list_object_CID} with {status_code}."
                )
                if logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=log_string,
                    )

            if peer_verified:
                break
    return status_code, peer_verified


def extract_peer_row_CID(
    call_stack,
    response_dict,
    logging_enabled,
):
    call_stack = call_stack + ":extract_peer_row_CID"
    try:
        peer_row_CID = response_dict[
            "peer_row_CID"
        ]  # from want item json file which is a pointer to the
        status_code = 200
    except KeyError:
        status_code = 800
        log_string = "dictionary did not contain a peer_row_CID."
        if logging_enabled:
            add_log(
                process=call_stack,
                peer_type="Error",
                msg=log_string,
            )

        peer_row_CID = "null"

    return status_code, peer_row_CID


if __name__ == "__main__":
    freeze_support()
    set_start_method("spawn")

    os.environ["DIYIMS_ROAMING"] = "RoamingDev"
    os.environ["COMPONENT_TEST"] = "0"
    os.environ["QUEUES_ENABLED"] = "0"
    os.environ["LOGGING_ENABLED"] = "1"

    config_dict = get_want_list_config_dict()
    peer_table_dict = {}
    peer_table_dict["peer_ID"] = "12D3KooWRwJtRqZQcvThkq2dU5ZbrS5zj6grE8rf4swG7NeFC3dH"
    peer_table_dict["peer_type"] = "PP"

    Args = WantlistCaptureProcessMainArgs(
        call_stack="__main__",
        peer_type="PP",
        want_list_config_dict=config_dict,
        peer_table_dict=peer_table_dict,
        queues_enabled=0,
        logging_enabled=1,
    )

    wantlist_capture_process_main(Args)
