# import json


# import requests

# from diyims.logger_utils import get_logger
# from diyims.ipfs_utils import get_url_dict
# from diyims.path_utils import get_path_dict, get_unique_file

# from diyims.config_utils import get_want_list_config_dict
# from diyims.requests_utils import execute_request

# from diyims.database_utils import (
#    set_up_sql_operations,
#    insert_header_row,
# )


def ipfs_header_add(
    call_stack,
    DTS,
    object_CID,
    object_type,
    peer_ID,
    config_dict,
    # logger,
    mode,
    # conn,
    # queries,
    processing_status,
    # Rconn,
    # Rqueries,
):
    from diyims.database_utils import insert_header_row, set_up_sql_operations
    from multiprocessing.managers import BaseManager
    from diyims.requests_utils import execute_request
    from diyims.path_utils import get_path_dict, get_unique_file
    from diyims.ipfs_utils import get_url_dict
    from diyims.logger_utils import add_log
    import json

    path_dict = get_path_dict()
    url_dict = get_url_dict()
    call_stack = call_stack + ":ipfs_header_add"

    if mode != "init":
        q_server_port = int(config_dict["q_server_port"])
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
        queue_server.register(
            "get_publish_queue"
        )  # NOTE: eventually pass which queue to use
        queue_server.connect()
        publish_queue = queue_server.get_publish_queue()
    conn, queries = set_up_sql_operations(config_dict)
    query_row = queries.select_last_header(conn, peer_ID=peer_ID)

    header_dict = {}
    header_dict["version"] = "0"
    header_dict["object_CID"] = object_CID
    header_dict["object_type"] = object_type
    header_dict["insert_DTS"] = DTS
    if query_row is None:
        header_dict["prior_header_CID"] = "null"
    else:
        header_dict["prior_header_CID"] = query_row["header_CID"]
    conn.close()
    header_dict["peer_ID"] = peer_ID
    header_dict["processing_status"] = processing_status

    proto_path = path_dict["header_path"]
    proto_file = path_dict["header_file"]
    proto_file_path = get_unique_file(proto_path, proto_file)

    param = {"cid-version": 1, "only-hash": "false", "pin": "true"}

    with open(proto_file_path, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(header_dict, write_file, indent=4)

    f = open(proto_file_path, "rb")
    add_file = {"file": f}
    response, status_code, response_dict = execute_request(
        url_key="add",
        # logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        param=param,
        file=add_file,
        call_stack=call_stack,
        http_500_ignore=False,
    )
    f.close()
    if status_code == 200:
        header_CID = response_dict["Hash"]
    else:
        add_log(
            process=call_stack,
            peer_type="Error",
            msg="IPFS Header Panic.",
        )
        return status_code, header_CID

    conn, queries = set_up_sql_operations(config_dict)
    insert_header_row(conn, queries, header_dict, header_CID)
    conn.commit()
    conn.close()

    if mode != "init":
        publish_queue.put_nowait("wake up")

    return status_code, header_CID


def ipfs_header_update(
    DTS,
    object_CID,
    object_type,
    peer_ID,
    config_dict,
    # logger,
    mode,
    # conn,
    # queries,
    processing_status,
    header_dict,
):
    from diyims.database_utils import insert_header_row, set_up_sql_operations
    from multiprocessing.managers import BaseManager

    if mode != "init":
        q_server_port = int(config_dict["q_server_port"])
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
        queue_server.register(
            "get_peer_maint_queue"
        )  # NOTE: eventually pass which queue to use
        queue_server.connect()
        peer_maint_queue = queue_server.get_peer_maint_queue()

    # query_row = queries.select_last_header(conn, peer_ID=peer_ID)
    conn, queries = set_up_sql_operations(config_dict)
    insert_header_row(conn, queries, header_dict)
    conn.commit()
    conn.close()

    if mode != "init":
        peer_maint_queue.put_nowait("wake up")

    return


def test_header_by_IPNS_name(IPNS_name):
    import requests
    from diyims.ipfs_utils import get_url_dict

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
