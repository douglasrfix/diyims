import json
import os
import sqlite3
from sqlite3 import Error

import aiosql
from rich import print

from diyims.database_utils import (
    insert_network_row,
    insert_peer_row,
    refresh_network_table_dict,
    refresh_peer_row_from_template,
)
from diyims.error_classes import (
    ApplicationNotInstalledError,
    CreateSchemaError,
    PreExistingInstallationError,
)
from diyims.general_utils import get_DTS, get_agent
from diyims.header_utils import ipfs_header_create
from diyims.ipfs_utils import get_url_dict, test_ipfs_version
from diyims.path_utils import get_path_dict
from diyims.platform_utils import get_python_version, test_os_platform
from diyims.py_version_dep import get_car_path, get_sql_str
from diyims.requests_utils import execute_request
from diyims.logger_utils import get_logger
from diyims.config_utils import get_db_init_config_dict


def create():
    try:
        path_dict = get_path_dict()

    except ApplicationNotInstalledError:
        raise

    sql_str = get_sql_str()
    queries = aiosql.from_str(sql_str, "sqlite3")
    connect_path = path_dict["db_file"]
    conn = sqlite3.connect(connect_path)

    try:
        queries.create_schema(conn)
        print("DB Schema creation successful.")

    except Error as e:
        conn.close()
        raise (CreateSchemaError(e))

    try:
        queries.set_pragma(conn)
        print("DB PRAGMA set successfully.")

    except Error as e:
        conn.close()
        raise (CreateSchemaError(e))

    conn.close()
    return


def init():  # NOTE: add wait on ipfs
    # NOTE: change header to pointer
    try:
        path_dict = get_path_dict()

    except ApplicationNotInstalledError:
        raise

    url_dict = get_url_dict()
    sql_str = get_sql_str()
    db_init_config_dict = get_db_init_config_dict()
    logger = get_logger(
        db_init_config_dict["log_file"],
        "none",
    )
    queries = aiosql.from_str(sql_str, "sqlite3")
    connect_path = path_dict["db_file"]
    conn = sqlite3.connect(connect_path)
    network_name = queries.select_network_name(conn)

    if network_name is not None:
        conn.close()
        raise (PreExistingInstallationError(" "))

    IPFS_agent = test_ipfs_version()
    os_platform = test_os_platform()
    agent = get_agent()

    try:
        python_version = os.environ["OVERRIDE_PYTHON_VERSION"]

    except KeyError:
        python_version = get_python_version()

    """
    DTS is the same for all artifacts of this transaction
    """
    DTS = get_DTS()

    print("This process can take several minutes. Have a cup of coffee.")

    response, status_code, response_dict = execute_request(
        url_key="id",
        logger=logger,
        url_dict=url_dict,
        config_dict=db_init_config_dict,
    )

    peer_ID = response_dict["ID"]

    sign_dict = {}
    sign_dict["peer_ID"] = peer_ID

    sign_file = path_dict["sign_file"]  # NOTE: generate unique name
    with open(sign_file, "w") as write_file:
        json.dump(sign_dict, write_file, indent=4)

    sign_files = {"file": open(sign_file, "rb")}
    sign_params = {}

    # NOTE: condition on ipfs agent
    response, status_code, response_dict = execute_request(
        url_key="sign",
        logger=logger,
        url_dict=url_dict,
        config_dict=db_init_config_dict,
        file=sign_files,
        param=sign_params,
    )

    id = response_dict["Key"]["Id"]  # also IPNS name
    signature = response_dict["Signature"]

    """
    Create the initial peer table entry for this peer.
    """

    DTS = get_DTS()

    peer_row_dict = refresh_peer_row_from_template()

    peer_row_dict["peer_ID"] = peer_ID
    peer_row_dict["IPNS_name"] = id
    peer_row_dict["signature"] = signature
    peer_row_dict["signature_valid"] = "true"
    peer_row_dict["peer_type"] = "LP"  # local provider peer
    peer_row_dict["origin_update_DTS"] = DTS
    peer_row_dict["local_update_DTS"] = DTS
    peer_row_dict["execution_platform"] = os_platform
    peer_row_dict["python_version"] = python_version
    peer_row_dict["IPFS_agent"] = IPFS_agent
    peer_row_dict["agent"] = agent
    peer_row_dict["processing_status"] = "NPC"  # Normal peer processing complete

    peer_file = path_dict["peer_file"]

    add_params = {"cid-version": 1, "only-hash": "false", "pin": "true"}
    with open(peer_file, "w") as write_file:
        json.dump(peer_row_dict, write_file, indent=4)
    add_files = {"file": open(peer_file, "rb")}

    response, status_code, response_dict = execute_request(
        url_key="add",
        logger=logger,
        url_dict=url_dict,
        config_dict=db_init_config_dict,
        file=add_files,
        param=add_params,
    )

    object_CID = response_dict["Hash"]
    object_type = (
        "peer_row_entry"  # NOTE: distinguish between local and remote entries in table
    )
    header_CID, IPNS_name = ipfs_header_create(DTS, object_CID, object_type)

    print(f"Header for the peer_row CID '{header_CID}'")

    # print(f"First network peer entry CID '{object_CID}'")
    network_table_dict = refresh_network_table_dict()
    network_table_dict["network_name"] = import_car()
    network_name = network_table_dict["network_name"]
    print(f"Network_name : '{network_name}'")  # NOTE: use logging

    object_CID = network_table_dict[
        "network_name"
    ]  # NOTE: Replace the need for this transaction by looking in the peer_table
    object_type = "network_name"
    header_CID, IPNS_name = ipfs_header_create(DTS, object_CID, object_type)

    insert_peer_row(conn, queries, peer_row_dict)
    conn.commit()

    insert_network_row(conn, queries, network_table_dict)
    conn.commit()

    conn.close()
    return


def import_car():
    url_dict = get_url_dict()
    db_init_config_dict = get_db_init_config_dict()
    logger = get_logger(
        db_init_config_dict["log_file"],
        "none",
    )

    car_path = get_car_path()
    dag_import_files = {"file": car_path}
    dag_import_params = {
        "pin-roots": "true",  # http status 500 if false but true does not pin given not in off-line mode
        "silent": "false",
        "stats": "false",
        "allow-big-block": "false",
    }

    response, status_code, response_dict = execute_request(
        url_key="dag_import",
        logger=logger,
        url_dict=url_dict,
        config_dict=db_init_config_dict,
        file=dag_import_files,
        param=dag_import_params,
    )

    imported_CID = response_dict["Root"]["Cid"]["/"]

    # import does not pin unless in offline mode so it must be done manually
    pin_add_params = {"arg": imported_CID}

    response, status_code, response_dict = execute_request(
        url_key="pin_add",
        logger=logger,
        url_dict=url_dict,
        config_dict=db_init_config_dict,
        file=dag_import_files,
        param=pin_add_params,
    )

    return imported_CID


if __name__ == "__main__":
    init()
