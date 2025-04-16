from datetime import datetime, timedelta, timezone

# from pathlib import Path
from rich import print
import json
import requests

from diyims.config_utils import get_want_list_config_dict

# from diyims.path_utils import get_path_dict
from diyims.database_utils import (
    set_up_sql_operations,
    refresh_peer_table_dict,
    select_peer_table_local_peer_entry,
    update_peer_table_metrics,
)
from diyims.requests_utils import execute_request
from diyims.general_utils import get_DTS
from diyims.platform_utils import get_python_version, test_os_platform
from diyims.ipfs_utils import get_url_dict, test_ipfs_version
from diyims.path_utils import get_path_dict


def filter_wantlist():
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

    print(query_start_dts)
    print(query_stop_dts)
    rows_of_wantlist_items = queries.select_filter_want_list_by_start_stop(
        conn,
        query_start_dts=query_start_dts,
        query_stop_dts=query_stop_dts,
    )

    for want_list_item in rows_of_wantlist_items:
        print(f"Object CID: {want_list_item['object_CID']}")
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
        response, status_code = execute_request(
            url_key,
            url_dict=url_dict,
            config_dict=want_list_config_dict,
            param=param,
            timeout=(3.05, 27),
        )
        if status_code == 200:
            print(response.headers["X-Content-Length"])
            X_Content_Length = int(response.headers["X-Content-Length"])

            if X_Content_Length >= 130 and X_Content_Length <= 170:
                start = response.text.find("{")
                print(start)
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

                        print(f"IPNS_name: {json_dict['IPNS_name']}")
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
                        print("Success")
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
    peer_table_dict = refresh_peer_table_dict()
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

    conn.close

    return


def test_sign_verify():
    from diyims.logger_utils import get_logger

    url_dict = get_url_dict()
    path_dict = get_path_dict()
    sign_dict = {}
    sign_file = path_dict["sign_file"]
    want_list_config_dict = get_want_list_config_dict()
    logger = get_logger(
        want_list_config_dict["log_file"],
        "none",
    )

    with requests.post(url_dict["id"], stream=False) as r:
        r.raise_for_status()
        json_dict = json.loads(r.text)

    sign_dict["peer_ID"] = json_dict["ID"]

    with open(sign_file, "w") as write_file:
        json.dump(sign_dict, write_file, indent=4)

    sign_files = {"file": open(sign_file, "rb")}
    sign_params = {}

    response, status_code = execute_request(
        url_key="sign",
        logger=logger,
        url_dict=url_dict,
        config_dict=want_list_config_dict,
        file=sign_files,
        param=sign_params,
    )

    json_dict = json.loads(response.text)
    print(json_dict["Key"]["Id"])
    print(json_dict["Signature"])

    id = json_dict["Key"]["Id"]
    signature = json_dict["Signature"]

    verify_files = {"file": open(sign_file, "rb")}
    verify_params = {"key": id, "signature": signature}

    response, status_code = execute_request(
        url_key="verify",
        logger=logger,
        url_dict=url_dict,
        config_dict=want_list_config_dict,
        file=verify_files,
        param=verify_params,
    )

    json_dict = json.loads(response.text)
    print(json_dict)

    return


if __name__ == "__main__":
    test_sign_verify()
