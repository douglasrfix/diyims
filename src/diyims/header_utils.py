import json


import requests

from diyims.logger_utils import get_logger
from diyims.ipfs_utils import get_url_dict
from diyims.path_utils import get_path_dict, get_unique_file

from diyims.config_utils import get_want_list_config_dict
from diyims.requests_utils import execute_request

from diyims.database_utils import (
    set_up_sql_operations,
)


def ipfs_header_create(DTS, object_CID, object_type, peer_ID):
    path_dict = get_path_dict()
    url_dict = get_url_dict()
    config_dict = get_want_list_config_dict()

    conn, queries = set_up_sql_operations(config_dict)

    # sql_str = get_sql_str()

    # connect_path = path_dict["db_file"]
    # conn = sqlite3.connect(connect_path)
    # conn.row_factory = sqlite3.Row
    # queries = aiosql.from_str(sql_str, "sqlite3")
    # query_row = queries.select_last_header(conn, peer_ID=peer_ID)

    header_dict = {}
    header_dict["version"] = "0"
    header_dict["object_CID"] = object_CID
    header_dict["object_type"] = object_type
    header_dict["insert_DTS"] = DTS
    header_dict["prior_header_CID"] = "null"
    header_dict["peer_ID"] = peer_ID
    header_dict["processing_status"] = "null"

    header_CID = "null"

    header_json_file = path_dict["header_file"]
    add_params = {"cid-version": 1, "only-hash": "false", "pin": "true"}

    with open(header_json_file, "w") as write_file:
        json.dump(header_dict, write_file, indent=4)

    f = open(header_json_file, "rb")
    add_files = {"file": f}
    with requests.post(url=url_dict["add"], params=add_params, files=add_files) as r:
        r.raise_for_status()
        json_dict = json.loads(r.text)
        header_CID = json_dict["Hash"]
    f.close()

    ipfs_path = "/ipfs/" + header_CID

    name_publish_arg = {
        "arg": ipfs_path,
        "resolve": "false",
        "lifetime": "1s",
        "ttl": "1s",
        "key": "self",
        "ipns-base": "base36",
    }

    with requests.post(
        url_dict["name_publish"],
        params=name_publish_arg,
        stream=False,  # TODO: #13 move name_publish to a separate task
    ) as r:
        r.raise_for_status()
        json_dict = json.loads(r.text)
        IPNS_name = json_dict["Name"]

    queries.insert_header_row(
        conn,
        version=header_dict["version"],
        object_CID=header_dict["object_CID"],
        object_type=header_dict["object_type"],
        insert_DTS=header_dict["insert_DTS"],
        prior_header_CID=header_dict["prior_header_CID"],
        header_CID=header_CID,
        peer_ID=header_dict["peer_ID"],
        processing_status=header_dict["processing_status"],
    )
    conn.commit()

    conn.close()

    return (header_CID, IPNS_name)


def ipfs_header_add(DTS, object_CID, object_type, peer_ID, config_dict, logger):
    from diyims.database_utils import insert_header_row
    from multiprocessing.managers import BaseManager

    path_dict = get_path_dict()
    url_dict = get_url_dict()

    q_server_port = int(config_dict["q_server_port"])
    queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
    queue_server.register("get_publish_queue")
    queue_server.connect()
    publish_queue = queue_server.get_publish_queue()

    conn, queries = set_up_sql_operations(config_dict)

    query_row = queries.select_last_header(conn, peer_ID=peer_ID)

    header_dict = {}
    header_dict["version"] = "0"
    header_dict["object_CID"] = object_CID
    header_dict["object_type"] = object_type
    header_dict["insert_DTS"] = DTS
    header_dict["prior_header_CID"] = query_row["header_CID"]
    header_dict["peer_ID"] = peer_ID
    header_dict["processing_status"] = "null"

    proto_path = path_dict["header_path"]
    proto_file = path_dict["header_file"]
    proto_file_path = get_unique_file(proto_path, proto_file)

    param = {"cid-version": 1, "only-hash": "false", "pin": "true"}

    with open(proto_file_path, "w") as write_file:
        json.dump(header_dict, write_file, indent=4)

    f = open(proto_file_path, "rb")
    add_file = {"file": f}
    response, status_code, response_dict = execute_request(
        url_key="add",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        param=param,
        file=add_file,
    )
    f.close()

    header_dict["header_CID"] = response_dict["Hash"]

    insert_header_row(conn, queries, header_dict)
    conn.commit()

    publish_queue.put_nowait("publish request")
    conn.close()

    return ()


def publish():
    from diyims.requests_utils import execute_request
    from multiprocessing.managers import BaseManager

    url_dict = get_url_dict()

    config_dict = get_want_list_config_dict()
    logger = get_logger(
        config_dict["log_file"],
        "none",
    )

    q_server_port = int(config_dict["q_server_port"])
    queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
    queue_server.register("get_publish_queue")
    queue_server.connect()
    publish_queue = queue_server.get_publish_queue()

    response, status_code, response_dict = execute_request(
        url_key="id",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
    )

    peer_ID = response_dict["ID"]

    conn, queries = set_up_sql_operations(config_dict)

    while True:
        query_row = queries.select_last_header(conn, peer_ID=peer_ID)
        # TODO: #15 add most recent publish
        header_CID = query_row["header_CID"]

        ipfs_path = "/ipfs/" + header_CID

        name_publish_arg = {
            "arg": ipfs_path,
            "resolve": "true",
            "key": "self",
            "ipns-base": "base36",
        }

        # TODO: #13 move name_publish to a separate task

        response, status_code, response_dict = execute_request(
            url_key="name_publish",
            logger=logger,
            url_dict=url_dict,
            config_dict=config_dict,
            param=name_publish_arg,
        )

        publish_queue.get()

    return


def test_header_by_IPNS_name(IPNS_name):
    # path_dict = get_path_dict()
    url_dict = get_url_dict()
    ipns_path = "/ipns/" + IPNS_name
    get_arg = {
        "arg": ipns_path,
    }

    with requests.post(url_dict["get"], params=get_arg, stream=False) as r:
        r.raise_for_status()
    print(r)
    print(r.text)
    return
