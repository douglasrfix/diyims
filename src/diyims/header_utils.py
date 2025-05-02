import json
import sqlite3

import aiosql
import requests

from diyims.ipfs_utils import get_url_dict
from diyims.path_utils import get_path_dict
from diyims.py_version_dep import get_sql_str


def ipfs_header_create(DTS, object_CID, object_type, peer_ID):
    path_dict = get_path_dict()
    url_dict = get_url_dict()

    sql_str = get_sql_str()

    connect_path = path_dict["db_file"]
    conn = sqlite3.connect(connect_path)
    conn.row_factory = sqlite3.Row
    queries = aiosql.from_str(sql_str, "sqlite3")
    query_row = queries.select_last_header(conn, peer_ID=peer_ID)

    if query_row is None:
        header_dict = {}
        header_dict["version"] = "0"
        header_dict["object_CID"] = object_CID
        header_dict["object_type"] = object_type
        header_dict["insert_DTS"] = DTS
        header_dict["prior_header_CID"] = "null"
        header_dict["peer_ID"] = peer_ID

        header_CID = "null"

    else:
        header_dict = {}
        header_dict["version"] = "0"
        header_dict["object_CID"] = object_CID
        header_dict["object_type"] = object_type
        header_dict["insert_DTS"] = DTS
        header_dict["prior_header_CID"] = query_row["header_CID"]
        header_dict["peer_ID"] = peer_ID

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
    if object_CID == "null":
        name_publish_arg = {
            "arg": ipfs_path,
            "resolve": "false",
            "lifetime": "1sec",
            "ttl": "1sec",
            "key": "self",
            "ipns-base": "base36",
        }
    else:
        name_publish_arg = {
            "arg": ipfs_path,
            "resolve": "true",
            "key": "self",
            "ipns-base": "base36",
        }

    with requests.post(
        url_dict["name_publish"], params=name_publish_arg, stream=False
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
    )
    conn.commit()

    conn.close()

    return (header_CID, IPNS_name)


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


def refresh_header_dict():
    header_dict = {}
    header_dict["version"] = "0"
    header_dict["object_CID"] = "null"
    header_dict["object_type"] = "null"
    header_dict["insert_DTS"] = "null"
    header_dict["prior_header_CID"] = "null"
    header_dict["peer_ID"] = "null"

    return header_dict
