import json
import os
from time import sleep
import requests
from requests.exceptions import HTTPError


from diyims.requests_utils import execute_request
from diyims.error_classes import UnSupportedIPFSVersionError
from diyims.py_version_dep import get_car_path
from diyims.database_utils import refresh_network_table_dict, select_network_name

from diyims.database_utils import set_up_sql_operations


def get_url_dict():
    url_dict = {}
    url_dict["add"] = "http://127.0.0.1:5001/api/v0/add"
    url_dict["get"] = "http://127.0.0.1:5001/api/v0/get"
    url_dict["cat"] = "http://127.0.0.1:5001/api/v0/cat"
    url_dict["id"] = "http://127.0.0.1:5001/api/v0/id"
    url_dict["sign"] = "http://127.0.0.1:5001/api/v0/key/sign"
    url_dict["verify"] = "http://127.0.0.1:5001/api/v0/key/verify"
    url_dict["dag_import"] = "http://127.0.0.1:5001/api/v0/dag/import"
    url_dict["name_publish"] = "http://127.0.0.1:5001/api/v0/name/publish"
    url_dict["find_providers"] = "http://127.0.0.1:5001/api/v0/routing/findprovs"
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


def purge():
    from diyims.config_utils import get_ipfs_config_dict
    from diyims.logger_utils import get_logger

    ipfs_config_dict = get_ipfs_config_dict()
    peer_type = "none"
    logger = get_logger(ipfs_config_dict["log_file"], peer_type)
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
                logger=logger,
                url_dict=url_dict,
                config_dict=ipfs_config_dict,
                param=param,
            )

        elif row["object_CID"] != "null":
            param = {"arg": row["object_CID"]}

            execute_request(
                url_key="pin_remove",
                logger=logger,
                url_dict=url_dict,
                config_dict=ipfs_config_dict,
                param=param,
            )

        param = {"arg": row["header_CID"]}

        execute_request(
            url_key="pin_remove",
            logger=logger,
            url_dict=url_dict,
            config_dict=ipfs_config_dict,
            param=param,
        )

    conn.close()

    with requests.post(url_dict["run_gc"], stream=False) as r:  # NOTE: fix
        r.raise_for_status()


def test_ipfs_version():
    url_dict = get_url_dict()

    with requests.post(url_dict["id"], stream=False) as r:  # NOTE: fix
        r.raise_for_status()
        json_dict = json.loads(r.text)

        supported_agents = [
            "kubo/0.34.1/",
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


def force_purge():
    from diyims.config_utils import get_ipfs_config_dict
    from diyims.logger_utils import get_logger

    ipfs_config_dict = get_ipfs_config_dict()
    url_dict = get_url_dict()
    peer_type = "none"
    logger = get_logger(ipfs_config_dict["log_file"], peer_type)

    with requests.post(url_dict["pin_list"], stream=False) as r:  # NOTE: fix
        r.raise_for_status()
        json_dict = json.loads(r.text)

        try:
            for key in json_dict["Keys"]:
                param = {"arg": key}
                execute_request(
                    url_key="pin_remove",
                    logger=logger,
                    url_dict=url_dict,
                    config_dict=ipfs_config_dict,
                    param=param,
                )

        except KeyError:
            pass

    with requests.post(url_dict["run_gc"], stream=False) as r:  # NOTE: fix
        r.raise_for_status()


def wait_on_ipfs(logger):  # NOTE: fix
    url_dict = get_url_dict()
    from diyims.config_utils import get_ipfs_config_dict

    ipfs_config_dict = get_ipfs_config_dict()
    i = 0
    not_found = True
    logger.debug("ipfs wait started.")
    sleep(int(ipfs_config_dict["connect_retry_delay"]))
    while i < 30 and not_found:
        try:
            with requests.post(url=url_dict["id"]) as r:
                r.raise_for_status()
                not_found = False
                logger.debug("ipfs wait completed.")
        except requests.exceptions.ConnectionError:
            i += 1
            logger.exception(f"wait on ipfs iteration {i}.")
            sleep(int(ipfs_config_dict["connect_retry_delay"]))

    return


def refresh_network_name():
    from diyims.config_utils import get_ipfs_config_dict
    from diyims.logger_utils import get_logger

    ipfs_config_dict = get_ipfs_config_dict()
    url_dict = get_url_dict()
    peer_type = "none"
    logger = get_logger(ipfs_config_dict["log_file"], peer_type)
    conn, queries = set_up_sql_operations(ipfs_config_dict)

    url_dict = get_url_dict()
    network_table_dict = refresh_network_table_dict()
    network_table_dict = select_network_name(conn, queries, network_table_dict)
    network_name = network_table_dict["network_name"]
    param = {"arg": network_name}

    logger.debug(f"refreshing {network_name}.")
    try:
        response, status_code, response_dict = execute_request(
            url_key="pin_remove",
            logger=logger,
            url_dict=url_dict,
            config_dict=ipfs_config_dict,
            param=param,
        )
    except HTTPError:
        logger.debug(response)

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
        logger=logger,
        url_dict=url_dict,
        config_dict=ipfs_config_dict,
        param=param,
        file=file,
    )
    logger.debug("refresh network name completed.")
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


def unpack_peer_row_from_cid(peer_row_CID, config_dict):
    url_dict = get_url_dict()
    param = {
        "arg": peer_row_CID,
    }
    url_key = "cat"

    response, status_code, response_dict = execute_request(
        url_key,
        url_dict=url_dict,
        config_dict=config_dict,
        param=param,
        timeout=(3.05, 27),
    )

    return response_dict


if __name__ == "__main__":
    refresh_network_name()
