# from datetime import datetime, timedelta, timezone

# from pathlib import Path
# from rich import print
# import requests

# from diyims.config_utils import get_want_list_config_dict

# from diyims.path_utils import get_path_dict
# from diyims.database_utils import (
#    set_up_sql_operations,
# )


def export_peer_table(
    conn,
    queries,
    url_dict,
    path_dict,
    config_dict,
    logger,
):
    """
    docstring
    """
    import json
    from diyims.path_utils import get_unique_file
    from diyims.requests_utils import execute_request

    # config_dict = get_want_list_config_dict()
    # conn, queries = set_up_sql_operations(config_dict)
    peer_table_rows = queries.select_peer_table_signature_valid(conn)
    peer_table_dict = {}
    for row in peer_table_rows:
        row_key_list = row.keys()

        peer_dict = {}
        for key in row_key_list:
            peer_dict[key] = row[key]

        # peer_table_dict["peer_ID"] = row["peer_ID"]
        peer_table_dict[row["peer_ID"]] = peer_dict
    print(peer_table_dict)
    # print(peer_dict)

    proto_path = path_dict["peer_path"]
    proto_file = path_dict["peer_file"]
    proto_file_path = get_unique_file(proto_path, proto_file)

    param = {"cid-version": 1, "only-hash": "false", "pin": "true"}
    with open(proto_file_path, "w") as write_file:
        json.dump(peer_table_dict, write_file, indent=4)

    f = open(proto_file_path, "rb")
    add_file = {"file": f}
    response, status_code, response_dict = execute_request(
        url_key="add",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        file=add_file,
        param=param,
    )
    f.close()

    object_CID = response_dict["Hash"]

    return object_CID


def test():
    export_peer_table()
    return


if __name__ == "__main__":
    test()
