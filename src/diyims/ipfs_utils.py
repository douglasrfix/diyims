# import json
# import os
# from time import sleep
# import requests
# from requests.exceptions import HTTPError


# from diyims.requests_utils import execute_request
# from diyims.error_classes import UnSupportedIPFSVersionError
# from diyims.py_version_dep import get_car_path
# from diyims.database_utils import (
#    refresh_network_table_from_template,
#    select_network_name,
# )

# from diyims.database_utils import set_up_sql_operations


def get_url_dict():
    url_dict = {}
    url_dict["add"] = "http://127.0.0.1:5001/api/v0/add"
    url_dict["get"] = "http://127.0.0.1:5001/api/v0/get"
    url_dict["cat"] = "http://127.0.0.1:5001/api/v0/cat"
    url_dict["id"] = "http://127.0.0.1:5001/api/v0/id"
    url_dict["resolve"] = "http://127.0.0.1:5001/api/v0/resolve"
    url_dict["sign"] = "http://127.0.0.1:5001/api/v0/key/sign"
    url_dict["verify"] = "http://127.0.0.1:5001/api/v0/key/verify"
    url_dict["dag_import"] = "http://127.0.0.1:5001/api/v0/dag/import"
    url_dict["name_publish"] = "http://127.0.0.1:5001/api/v0/name/publish"
    url_dict["find_providers"] = "http://127.0.0.1:5001/api/v0/routing/findprovs"
    url_dict["provide"] = "http://127.0.0.1:5001/api/v0/routing/provide"
    url_dict["find_peer"] = "http://127.0.0.1:5001/api/v0/routing/findpeer"
    url_dict["pin_list"] = "http://127.0.0.1:5001/api/v0/pin/ls"
    url_dict["pin_add"] = "http://127.0.0.1:5001/api/v0/pin/add"
    url_dict["pin_remove"] = "http://127.0.0.1:5001/api/v0/pin/rm"
    url_dict["run_gc"] = "http://127.0.0.1:5001/api/v0/repo/gc"
    url_dict["want_list"] = "http://127.0.0.1:5001/api/v0/bitswap/wantlist"
    url_dict["bitswap_stat"] = "http://127.0.0.1:5001/api/v0/bitswap/stat"
    url_dict["swarm_peers"] = "http://127.0.0.1:5001/api/v0/swarm/peers"
    url_dict["connect"] = "http://127.0.0.1:5001/api/v0/swarm/connect"
    url_dict["dis_connect"] = "http://127.0.0.1:5001/api/v0/swarm/disconnect"
    url_dict["peering_add"] = "http://127.0.0.1:5001/api/v0/swarm/peering/add"
    url_dict["peering_remove"] = (
        "http://127.0.0.1:5001/api/v0/swarm/peering/rm"  # NOTE: config enable of peering
    )

    return url_dict


def purge(call_stack):
    from diyims.config_utils import get_ipfs_config_dict

    # from diyims.logger_utils import get_logger
    from diyims.requests_utils import execute_request
    from diyims.database_utils import set_up_sql_operations
    import requests

    call_stack = call_stack + ":purge"
    ipfs_config_dict = get_ipfs_config_dict()
    # peer_type = "none"
    # logger = get_logger(ipfs_config_dict["log_file"], peer_type)
    url_dict = get_url_dict()
    conn, queries = set_up_sql_operations(ipfs_config_dict)

    # header_table_dict = get_header_table_dict()
    header_table_rows = queries.select_all_headers(conn)

    for row in header_table_rows:
        # print(f"header_CID '{row['header_CID']}'")
        if row["object_type"] == "IPNS_name":
            print(
                f"IPNS_name '{row['object_CID']}'"
            )  # NOTE: needs a function to name in danger
            header_CID = row["object_CID"]

            ipfs_path = "/ipfs/" + header_CID

            param = {
                "arg": ipfs_path,
                "resolve": "false",
                "lifetime": "10s",
                "ttl": "10s",
                "key": "self",
                "ipns-base": "base36",
            }

            execute_request(
                url_key="name_publish",
                # =logger,
                url_dict=url_dict,
                config_dict=ipfs_config_dict,
                param=param,
                call_stack=call_stack,
            )
            # TODO: 700 later
        elif row["object_CID"] != "null":
            param = {"arg": row["object_CID"]}

            execute_request(
                url_key="pin_remove",
                # logger=logger,
                url_dict=url_dict,
                config_dict=ipfs_config_dict,
                param=param,
                call_stack=call_stack,
            )
            # TODO: 700 later
        param = {"arg": row["header_CID"]}

        execute_request(
            url_key="pin_remove",
            # logger=logger,
            url_dict=url_dict,
            config_dict=ipfs_config_dict,
            param=param,
            call_stack=call_stack,
        )
        # TODO: 700 later
    conn.close()

    with requests.post(url_dict["run_gc"], stream=False) as r:  # NOTE: fix
        r.raise_for_status()


def test_ipfs_version():
    import json
    import os
    import requests
    from diyims.error_classes import UnSupportedIPFSVersionError

    url_dict = get_url_dict()

    with requests.post(url_dict["id"], stream=False) as r:  # NOTE: fix
        r.raise_for_status()
        json_dict = json.loads(r.text)

        supported_agents = [
            "kubo/0.34.1/",
            "kubo/0.35.0/",
            "kubo/0.36.0/",
            "kubo/0.37.0/",
        ]
        match_count = 0
        for x in supported_agents:
            if json_dict["AgentVersion"] not in x:
                pass
            else:
                match_count = match_count + 1

        try:
            match_count = int(os.environ["OVERRIDE_IPFS_VERSION"])

        except KeyError:
            pass

        if match_count == 0:
            raise (UnSupportedIPFSVersionError(json_dict["AgentVersion"]))

    return json_dict["AgentVersion"]


def force_purge(call_stack):
    import json
    import requests
    from diyims.config_utils import get_ipfs_config_dict

    # from diyims.logger_utils import get_logger
    from diyims.requests_utils import execute_request

    call_stack = call_stack + ":force_purge"
    ipfs_config_dict = get_ipfs_config_dict()
    url_dict = get_url_dict()
    # peer_type = "none"
    # logger = get_logger(ipfs_config_dict["log_file"], peer_type)

    with requests.post(url_dict["pin_list"], stream=False) as r:  # NOTE: fix
        r.raise_for_status()
        json_dict = json.loads(r.text)

        try:
            for key in json_dict["Keys"]:
                param = {"arg": key}
                execute_request(
                    url_key="pin_remove",
                    # logger=logger,
                    url_dict=url_dict,
                    config_dict=ipfs_config_dict,
                    param=param,
                    call_stack=call_stack,
                )
                # TODO: 700 later
        except KeyError:
            pass

    with requests.post(url_dict["run_gc"], stream=False) as r:  # NOTE: fix
        r.raise_for_status()


def wait_on_ipfs(call_stack):  # TODO: redo this logic to use exec
    from time import sleep
    import requests
    from diyims.config_utils import get_ipfs_config_dict

    call_stack = call_stack + "wait_on_ipfs"
    url_dict = get_url_dict()
    ipfs_config_dict = get_ipfs_config_dict()
    i = 0
    not_found = True
    # logger.debug("ipfs wait started.")
    sleep(int(ipfs_config_dict["connect_retry_delay"]))
    while i < 30 and not_found:
        try:
            with requests.post(url=url_dict["id"]) as r:
                r.raise_for_status()
                not_found = False
                # logger.debug("ipfs wait completed.")
        except requests.exceptions.ConnectionError:
            i += 1
            # logger.exception(f"wait on ipfs iteration {i}.")
            sleep(int(ipfs_config_dict["connect_retry_delay"]))

    return


def refresh_network_name(call_stack):
    import requests
    from requests.exceptions import HTTPError
    from diyims.config_utils import get_ipfs_config_dict

    # from diyims.logger_utils import add_log
    from diyims.requests_utils import execute_request
    from diyims.database_utils import set_up_sql_operations
    from diyims.database_utils import (
        refresh_network_table_from_template,
        select_network_name,
    )
    from diyims.py_version_dep import get_car_path

    call_stack = call_stack + ":refresh_network_name"
    ipfs_config_dict = get_ipfs_config_dict()
    url_dict = get_url_dict()
    # = "none"
    # logger = get_logger(ipfs_config_dict["log_file"], peer_type)
    conn, queries = set_up_sql_operations(ipfs_config_dict)

    url_dict = get_url_dict()
    network_table_dict = refresh_network_table_from_template()
    network_table_dict = select_network_name(conn, queries, network_table_dict)
    network_name = network_table_dict["network_name"]
    param = {"arg": network_name}

    # logger.debug(f"refreshing {network_name}.")
    try:
        response, status_code, response_dict = execute_request(
            url_key="pin_remove",
            # logger=logger,
            url_dict=url_dict,
            config_dict=ipfs_config_dict,
            param=param,
            call_stack=call_stack,
        )
        # TODO: 700 later
    except HTTPError:
        # logger.debug(response)
        pass

    with requests.post(url_dict["run_gc"], stream=False) as r:  # NOTE: Fix
        r.raise_for_status()

    car_path = get_car_path()
    file = {"file": car_path}
    param = {
        "pin-roots": "true",  # http status 500 if false but true does not pin given not in off-line mode
        "silent": "false",
        "stats": "false",
        "allow-big-block": "false",
    }

    execute_request(
        url_key="dag_import",
        # logger=logger,
        url_dict=url_dict,
        config_dict=ipfs_config_dict,
        param=param,
        file=file,
        call_stack=call_stack,
    )  # TODO: 700 later
    # logger.debug("refresh network name completed.")
    conn.close()

    """
    json_dict = json.loads(r.text)
    imported_CID = json_dict["Root"]["Cid"]["/"]

        # import does not pin unless in offline mode so it must be done manually
    pin_add_params = {"arg": imported_CID}
    with requests.post(
            url_dict["pin_add"], params=pin_add_params, stream=False
        ) as r:
        r.raise_for_status()
    print(r)
    print("add pin")
    """
    return


def unpack_peer_row_from_cid(call_stack, peer_row_CID, config_dict):
    from diyims.requests_utils import execute_request
    from diyims.logger_utils import add_log

    call_stack = call_stack + "unpack_peer_row_from_cid"
    param = {
        "arg": peer_row_CID,
    }

    response, status_code, response_dict = execute_request(
        url_key="cat",
        config_dict=config_dict,
        param=param,
        timeout=(3.05, 122),  # control time out to avoid retries
        call_stack=call_stack,
        http_500_ignore=False,
    )
    if status_code != 200:
        add_log(
            process=call_stack,
            peer_type="Error",
            msg="Unpack exhausted retries.",
        )
    return status_code, response_dict


def export_peer_table(
    call_stack,
    # conn,
    # queries,
    url_dict,
    path_dict,
    config_dict,
    # logger,
):
    """
    docstring
    """
    import json
    from diyims.path_utils import get_unique_file
    from diyims.database_utils import set_up_sql_operations
    from diyims.requests_utils import execute_request
    from diyims.logger_utils import add_log

    call_stack = call_stack + ":export_peer_table"
    conn, queries = set_up_sql_operations(config_dict)
    peer_table_rows = queries.select_peer_table_signature_valid(conn)
    peer_table_dict = {}
    for row in peer_table_rows:
        row_key_list = row.keys()

        peer_dict = {}
        for key in row_key_list:
            peer_dict[key] = row[key]

        peer_table_dict[row["peer_ID"]] = peer_dict
    conn.close()
    proto_path = path_dict["peer_path"]
    proto_file = path_dict["peer_file"]
    proto_file_path = get_unique_file(proto_path, proto_file)

    param = {"cid-version": 1, "only-hash": "false", "pin": "true"}
    with open(proto_file_path, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(peer_table_dict, write_file, indent=4)

    f = open(proto_file_path, "rb")
    add_file = {"file": f}
    response, status_code, response_dict = execute_request(
        url_key="add",
        # =logger,
        url_dict=url_dict,
        config_dict=config_dict,
        file=add_file,
        param=param,
        call_stack=call_stack,
        ignore_HTTP500=False,
    )
    f.close()

    if status_code == 200:
        object_CID = response_dict["Hash"]
    else:
        add_log(
            process=call_stack,
            peer_type="Error",
            msg="Export Peer Panic.",
        )
        return status_code, object_CID

    return status_code, object_CID


if __name__ == "__main__":
    import os
    from multiprocessing import set_start_method, freeze_support

    freeze_support()
    set_start_method("spawn")

    os.environ["DIYIMS_ROAMING"] = "RoamingDev"
    os.environ["COMPONENT_TEST"] = "1"
    os.environ["QUEUES_ENABLED"] = "0"
    # ("__main__", "init")
