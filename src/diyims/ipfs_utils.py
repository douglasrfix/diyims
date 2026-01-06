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
            "kubo/0.37.0/",
            "kubo/0.39.0/",
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


def unpack_object_from_cid(
    call_stack,
    object_CID,
):
    from diyims.requests_utils import execute_request
    from diyims.logger_utils import add_log

    call_stack = call_stack + "unpack_object_from_cid"
    param = {
        "arg": object_CID,
    }

    response, status_code, response_dict = execute_request(
        url_key="cat",
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


if __name__ == "__main__":
    import os
    from multiprocessing import set_start_method, freeze_support

    freeze_support()
    set_start_method("spawn")

    os.environ["DIYIMS_ROAMING"] = "RoamingDev"
    os.environ["COMPONENT_TEST"] = "1"
    os.environ["QUEUES_ENABLED"] = "0"
    # ("__main__", "init")
