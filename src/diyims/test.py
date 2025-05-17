# from datetime import datetime, timedelta, timezone

# from pathlib import Path
from rich import print
# import requests

# from diyims.config_utils import get_want_list_config_dict

# from diyims.path_utils import get_path_dict
# from diyims.database_utils import (
#    set_up_sql_operations,
# )


def find_peer_test():
    """
    docstring
    """
    # import json
    # from diyims.path_utils import get_unique_file
    from diyims.requests_utils import execute_request
    from diyims.logger_utils import get_logger
    from diyims.ipfs_utils import get_url_dict
    from diyims.config_utils import get_want_list_config_dict
    import json

    config_dict = get_want_list_config_dict()
    logger = get_logger(
        config_dict["log_file"],
        "none",
    )
    url_dict = get_url_dict()

    response, status_code, response_dict = execute_request(
        url_key="id",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
    )

    self = response_dict["ID"]

    param = {"arg": "QmWXemkANSxL54BApBjXDsu9137YDaUiwRhRxahhsLook6"}

    response, status_code, response_dict = execute_request(
        url_key="find_providers",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        param=param,
    )
    line_list = []
    for line in response.iter_lines():
        line_list.append(line)

    for line in line_list:
        # for line in response.iter_lines():
        decoded_line = line.decode("utf-8")  # to translate b string to character
        line_dict = json.loads(
            decoded_line
        )  # could be done just using lists but is more transparent with dictionaries

        if line_dict["Type"] == 4:
            responses_list = line_dict["Responses"]
            for response in responses_list:
                list_of_peer_dict = str(response).replace(
                    "'", '"'
                )  # needed to use json
                peer_dict = json.loads(str(list_of_peer_dict))
                peer_ID = peer_dict["ID"]
                if self != peer_ID:
                    print(peer_ID)
                    address_list = peer_dict["Addrs"]
                    print(address_list)
                    for address in address_list:
                        if address[:5] == "/ip4/":
                            if address[:7] != "/ip4/10":
                                if address[:8] != "/ip4/192":  # 172.16 thru 172.31
                                    if address[:8] != "/ip4/127":
                                        index = address.lower().find("/tcp/")
                                        port_start = index + 5
                                        port = address[port_start:]
                                        if port.isnumeric():
                                            print(
                                                "A",
                                                index,
                                                port,
                                                address,
                                            )
                                            connect_address = (
                                                address + "/p2p/" + peer_ID
                                            )
                                            print("B", connect_address)

                                            # if address[:5] == "/ip6/":
                                            # print("B", address)

                                            param = {"arg": connect_address}

                                            response, status_code, response_dict = (
                                                execute_request(
                                                    url_key="peering_add",
                                                    logger=logger,
                                                    url_dict=url_dict,
                                                    config_dict=config_dict,
                                                    param=param,
                                                )
                                            )

                                            print(status_code, response_dict)

                                            param = {"arg": connect_address}

                                            response, status_code, response_dict = (
                                                execute_request(
                                                    url_key="connect",
                                                    logger=logger,
                                                    url_dict=url_dict,
                                                    config_dict=config_dict,
                                                    param=param,
                                                )
                                            )

                                            print(status_code, response_dict)

                                            response, status_code, response_dict = (
                                                execute_request(
                                                    url_key="dis_connect",
                                                    logger=logger,
                                                    url_dict=url_dict,
                                                    config_dict=config_dict,
                                                    param=param,
                                                )
                                            )

                                            print(status_code, response_dict)

                                            param = {"arg": peer_ID}

                                            response, status_code, response_dict = (
                                                execute_request(
                                                    url_key="peering_remove",
                                                    logger=logger,
                                                    url_dict=url_dict,
                                                    config_dict=config_dict,
                                                    param=param,
                                                )
                                            )

                                            print(status_code, response_dict)

    """    for line in line_list:
        decoded_line = line.decode("utf-8")
        line_dict = json.loads(decoded_line)

        if line_dict["Type"] != 4:
            if peer_ID == line_dict["ID"]:
            #if peer_ID == peer_ID:
                responses_list = line_dict["Responses"]
                for response in responses_list:

                    list_of_peer_dict = str(response).replace("'", '"')
                    peer_dict = json.loads(str(list_of_peer_dict))
                    print(peer_dict["ID"])
                    if peer_ID == peer_dict["ID"]:
                        address_list = peer_dict["Addrs"]
                        for address in address_list:
                            #if address[:5] == "/ip4/":
                            print("B", address)
    """

    return


def test():
    find_peer_test()
    return


if __name__ == "__main__":
    test()
