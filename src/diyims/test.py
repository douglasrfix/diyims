from datetime import datetime, timedelta, timezone

# from pathlib import Path
from rich import print
import json
import requests

from diyims.config_utils import get_want_list_config_dict

# from diyims.path_utils import get_path_dict
from diyims.database_utils import (
    set_up_sql_operations,
    refresh_peer_row_from_template,
    select_peer_table_local_peer_entry,
    update_peer_table_metrics,
)
from diyims.requests_utils import execute_request
from diyims.general_utils import get_DTS
from diyims.platform_utils import get_python_version, test_os_platform
from diyims.ipfs_utils import get_url_dict, test_ipfs_version
from diyims.path_utils import get_path_dict
from diyims.logger_utils import get_logger


def filter_wantlist():
    # from diyims.security_utils import verify_peer_row_from_cid
    want_list_config_dict = get_want_list_config_dict()
    logger = get_logger(
        want_list_config_dict["log_file"],
        "none",
    )
    # path_dict = get_path_dict()

    conn, queries = set_up_sql_operations(want_list_config_dict)

    filter_wantlist_test("0", conn, queries, logger, want_list_config_dict)


def filter_wantlist_test(pid, conn, queries, logger, config_dict):
    from diyims.ipfs_utils import unpack_peer_row_from_cid
    from diyims.database_utils import (
        refresh_log_dict,
        insert_log_row,
        update_peer_row_by_key_status,
    )

    current_DT = datetime.now(timezone.utc)
    start_off_set = timedelta(hours=10)
    window_duration = timedelta(hours=10)
    start_dts = current_DT - start_off_set
    end_dts = start_dts + window_duration
    query_start_dts = datetime.isoformat(start_dts)
    query_stop_dts = datetime.isoformat(end_dts)
    largest_delta = 295
    smallest_delta = 240
    x_content_min = 130
    x_content_max = 170

    rows_of_wantlist_items = queries.select_filter_want_list_by_start_stop(
        conn,
        query_start_dts=query_start_dts,
        query_stop_dts=query_stop_dts,
        largest_delta=largest_delta,
        smallest_delta=smallest_delta,
    )

    for want_list_item_row in rows_of_wantlist_items:
        want_list_object_CID = want_list_item_row[
            "object_CID"
        ]  # This is the json file containing the peer row cid

        log_string = f"Object {want_list_object_CID} found between {query_start_dts} and {query_stop_dts}."

        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "wantlist-filter-N1"
        log_dict["pid"] = pid
        log_dict["peer_type"] = "PP"
        log_dict["msg"] = log_string
        insert_log_row(conn, queries, log_dict)
        conn.commit()

        url_dict = get_url_dict()
        param = {
            "arg": want_list_object_CID,  # json file
        }
        url_key = "cat"

        response, status_code, response_dict = execute_request(
            url_key,
            url_dict=url_dict,
            config_dict=config_dict,
            param=param,
            timeout=(3.05, 27),
        )

        if status_code == 200:
            X_Content_Length = int(response.headers["X-Content-Length"])

            log_string = f"CAT result {status_code} with dictionary of {response_dict} with {X_Content_Length}."

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
                peer_row_CID = extract_peer_row_CID(response_dict, conn, queries, pid)

                if peer_row_CID != "null":
                    try:
                        peer_row_CID = response_dict[
                            "IPNS_name"
                        ]  # from want item json file
                        version = 0

                    except KeyError:
                        peer_row_CID = response_dict[
                            "peer_row_CID"
                        ]  # from want item json file
                        version = 1

                    if version == 1:
                        verify_peer_and_update(
                            peer_row_CID, logger, config_dict, conn, queries, pid
                        )

                    else:
                        peer_row_dict = unpack_peer_row_from_cid(
                            peer_row_CID, config_dict
                        )

                        # Uconn, Uqueries = set_up_sql_operations(config_dict)
                        peer_row_dict["processing_status"] = "NPC"
                        peer_row_dict["local_update_DTS"] = get_DTS()
                        # peer_row_dict["peer_ID"] = want_list_item["peer_ID"]

                        update_peer_row_by_key_status(
                            conn, queries, peer_row_dict
                        )  # TODO: handling sql exceptions using aiosql?

                        conn.commit()
                        # Uconn.close()

                        # print("Success")
                        log_string = f"Peer v0 {peer_row_dict['IPNS_name']}  updated."

                        log_dict = refresh_log_dict()
                        log_dict["DTS"] = get_DTS()
                        log_dict["process"] = "wantlist-filter-N4"
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

    # conn.close()
    return


def extract_peer_row_CID(response_dict, conn, queries, pid):
    from diyims.database_utils import refresh_log_dict, insert_log_row

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
        insert_log_row(conn, queries, log_dict)
        conn.commit()
        peer_row_CID = "null"

        try:
            peer_row_CID = response_dict["IPNS_name"]  # from want item json file
        except KeyError:
            log_string = "dictionary did not contain an IPNS_name."

            log_dict = refresh_log_dict()
            log_dict["DTS"] = get_DTS()
            log_dict["process"] = "wantlist-filter-F4"
            log_dict["pid"] = pid
            log_dict["peer_type"] = "PP"
            log_dict["msg"] = log_string
            insert_log_row(conn, queries, log_dict)
            conn.commit()
            peer_row_CID = "null"

    return peer_row_CID


def verify_peer_and_update(peer_row_CID, logger, config_dict, conn, queries, pid):
    from diyims.security_utils import verify_peer_row_from_cid
    from diyims.database_utils import update_peer_row_by_key_status_v1
    from diyims.database_utils import refresh_log_dict, insert_log_row

    peer_verified, peer_row_dict = verify_peer_row_from_cid(
        peer_row_CID, logger, config_dict
    )

    # Read here for current status
    if peer_verified:
        # Uconn, Uqueries = set_up_sql_operations(config_dict)
        peer_row_dict["processing_status"] = "NPC"
        peer_row_dict["local_update_DTS"] = get_DTS()
        # peer_row_dict["peer_ID"] = want_list_item["peer_ID"]

        update_peer_row_by_key_status_v1(
            conn, queries, peer_row_dict
        )  # TODO: handling sql exceptions using aiosql?

        conn.commit()
        # Uconn.close()

        # print("Success")
        log_string = f"Peer v1 {peer_row_dict['peer_ID']}  updated."

        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "wantlist-filter-N3"
        log_dict["pid"] = pid
        log_dict["peer_type"] = "PP"
        log_dict["msg"] = log_string
        insert_log_row(conn, queries, log_dict)
        conn.commit()

    else:
        log_string = f"Peer {peer_row_dict['peer_entry_CID']} not valid."

        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "wantlist-filter-F5"
        log_dict["pid"] = pid
        log_dict["peer_type"] = "PP"
        log_dict["msg"] = log_string
        insert_log_row(conn, queries, log_dict)
        conn.commit()

    return


def decode_find_peer_structure(
    logger,
    conn,
    queries,
    capture_peer_config_dict,
    url_dict,
    response,
    peer_queue,
):
    for line in (
        response.iter_lines()
    ):  # NOTE: make this return a list to be processed by a database function
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
                    # the source for this is dht which may be present with out an address
                    address_available = True

                except IndexError:
                    address_available = False

    return address_available, peer_address


def select_local_peer_and_update_metrics():
    want_list_config_dict = get_want_list_config_dict()

    DTS = get_DTS()
    conn, queries = set_up_sql_operations(want_list_config_dict)
    peer_table_dict = refresh_peer_row_from_template()
    peer_table_entry = select_peer_table_local_peer_entry(
        conn, queries, peer_table_dict
    )

    IPFS_agent = test_ipfs_version()
    os_platform = test_os_platform()
    python_version = get_python_version()
    agent = "0.0.0a49"
    changed_metrics = False

    if peer_table_entry["execution_platform"] != os_platform:
        peer_table_dict["execution_platform"] = os_platform
        changed_metrics = True
    else:
        peer_table_dict["execution_platform"] = os_platform

    if peer_table_entry["python_version"] != python_version:
        peer_table_dict["python_version"] = python_version
        changed_metrics = True
    else:
        peer_table_dict["python_version"] = python_version

    if peer_table_entry["IPFS_agent"] != IPFS_agent:
        peer_table_dict["IPFS_agent"] = IPFS_agent
        changed_metrics = True
    else:
        peer_table_dict["IPFS_agent"] = IPFS_agent

    if peer_table_entry["agent"] != agent:
        peer_table_dict["agent"] = agent
        changed_metrics = True
    else:
        peer_table_dict["agent"] = agent

    if changed_metrics:
        peer_table_dict["origin_update_DTS"] = DTS
        update_peer_table_metrics(conn, queries, peer_table_dict)
        conn.commit()
        print("metrics changed")

    conn.close()

    return


def test_sign_verify():
    from diyims.logger_utils import get_logger
    from diyims.security_utils import sign_file, verify_file

    url_dict = get_url_dict()
    path_dict = get_path_dict()
    sign_dict = {}
    signing_dict = {}
    file_to_sign = path_dict["sign_file"]
    want_list_config_dict = get_want_list_config_dict()
    logger = get_logger(
        want_list_config_dict["log_file"],
        "none",
    )

    with requests.post(url_dict["id"], stream=False) as r:
        r.raise_for_status()
        json_dict = json.loads(r.text)

    signing_dict["peer_ID"] = json_dict["ID"]

    sign_dict["file_to_sign"] = file_to_sign

    with open(file_to_sign, "w") as write_file:
        json.dump(signing_dict, write_file, indent=4)

    id, signature = sign_file(sign_dict, logger, want_list_config_dict)

    verify_dict = {}
    verify_dict["signed_file"] = file_to_sign
    verify_dict["id"] = id
    verify_dict["signature"] = signature

    signature_valid = verify_file(verify_dict, logger, want_list_config_dict)
    if signature_valid:
        print(signature_valid)
    return


def test_verify_peer():
    from diyims.security_utils import verify_peer_row_from_cid
    from diyims.logger_utils import get_logger

    want_list_config_dict = get_want_list_config_dict()
    logger = get_logger(
        want_list_config_dict["log_file"],
        "none",
    )
    peer_row_CID = ""

    peer_verified, peer_row_dict = verify_peer_row_from_cid(
        peer_row_CID, logger, want_list_config_dict
    )
    print(peer_verified)
    print(peer_row_dict)

    return


def unpack_peer_row_from_cid():
    want_list_config_dict = get_want_list_config_dict()
    url_dict = get_url_dict()
    peer_row_CID = ""
    param = {
        "arg": peer_row_CID,
    }
    url_key = "cat"

    response, status_code, response_dict = execute_request(
        url_key,
        url_dict=url_dict,
        config_dict=want_list_config_dict,
        param=param,
        timeout=(3.05, 27),
    )

    return response_dict


if __name__ == "__main__":
    filter_wantlist()
