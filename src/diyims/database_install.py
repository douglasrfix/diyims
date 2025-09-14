import json
import os
from rich import print

from diyims.database_utils import (
    insert_network_row,
    insert_peer_row,
    refresh_network_table_from_template,
    refresh_peer_row_from_template,
    set_up_sql_operations,
    add_shutdown_entry,
)
from diyims.error_classes import (
    ApplicationNotInstalledError,
    PreExistingInstallationError,
)
from diyims.general_utils import get_DTS, get_agent
from diyims.header_utils import ipfs_header_add
from diyims.ipfs_utils import get_url_dict, test_ipfs_version, wait_on_ipfs
from diyims.path_utils import get_path_dict, get_unique_file
from diyims.platform_utils import get_python_version, test_os_platform
from diyims.py_version_dep import get_car_path
from diyims.requests_utils import execute_request

# from diyims.logger_utils import get_logger
from diyims.config_utils import get_db_init_config_dict
from diyims.security_utils import sign_file, verify_file
from sqlmodel import SQLModel, create_engine, Session, text


def create(call_stack):
    try:
        path_dict = get_path_dict()

    except ApplicationNotInstalledError:
        raise

    call_stack = call_stack + ":create"
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"
    print(db_url)

    engine = create_engine(db_url, echo=False, connect_args={"timeout": 10})
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session = Session(engine)
        statement = text("PRAGMA journal_mode = WAL;")
        session.exec(statement)
        print("DB Schema creation successful.")

    return


def init(call_stack):
    call_stack = call_stack + ":init"
    try:
        path_dict = get_path_dict()

    except ApplicationNotInstalledError:
        raise

    url_dict = get_url_dict()

    config_dict = get_db_init_config_dict()
    # logger = get_logger(
    #    config_dict["log_file"],
    #    "none",
    # )
    wait_on_ipfs(call_stack)

    conn, queries = set_up_sql_operations(config_dict)
    Rconn, Rqueries = set_up_sql_operations(config_dict)

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
        # logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        call_stack=call_stack,
        http_500_ignore=False,
    )

    peer_ID = response_dict["ID"]
    # TODO: 700 later
    signing_dict = {}
    signing_dict["peer_ID"] = peer_ID

    proto_path = path_dict["sign_path"]
    proto_file = path_dict["sign_file"]
    proto_file_path = get_unique_file(proto_path, proto_file)

    with open(proto_file_path, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(signing_dict, write_file, indent=4)

    sign_dict = {}
    sign_dict["file_to_sign"] = proto_file_path

    status_code, id, signature = sign_file(call_stack, sign_dict, config_dict)

    verify_dict = {}
    verify_dict["signed_file"] = proto_file_path
    verify_dict["id"] = id
    verify_dict["signature"] = signature

    status_code, signature_valid = verify_file(call_stack, verify_dict, config_dict)

    """
    Create the initial peer table entry for this peer.
    """

    DTS = get_DTS()

    peer_row_dict = refresh_peer_row_from_template()

    proto_path = path_dict["peer_path"]
    proto_file = path_dict["peer_file"]
    proto_file_path = get_unique_file(proto_path, proto_file)

    peer_file = proto_file_path

    add_params = {"cid-version": 1, "only-hash": "false", "pin": "false"}
    with open(peer_file, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(peer_row_dict, write_file, indent=4)

    f = open(peer_file, "rb")
    add_files = {"file": f}
    response, status_code, response_dict = (
        execute_request(  # to have something to publish to capture IPNS_name
            url_key="add",
            # logger=logger,
            url_dict=url_dict,
            config_dict=config_dict,
            file=add_files,
            param=add_params,
            call_stack=call_stack,
        )
    )
    f.close()
    # TODO: 700 later
    object_CID = response_dict["Hash"]

    ipfs_path = "/ipfs/" + object_CID

    name_publish_arg = {
        "arg": ipfs_path,
        "resolve": "false",
        "lifetime": "1s",
        "ttl": "1s",
        "key": "self",
        "ipns-base": "base36",
    }

    response, status_code, response_dict = execute_request(  # publish to get ipns name
        url_key="name_publish",
        # logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        param=name_publish_arg,
        call_stack=call_stack,
        http_500_ignore=False,
    )
    # TODO: 700 later
    IPNS_name = response_dict["Name"]

    peer_row_dict["peer_ID"] = peer_ID
    peer_row_dict["IPNS_name"] = IPNS_name  # capture ipns_name
    peer_row_dict["id"] = id
    peer_row_dict["signature"] = signature
    peer_row_dict["signature_valid"] = signature_valid
    peer_row_dict["peer_type"] = "LP"  # local provider peer
    peer_row_dict["origin_update_DTS"] = DTS
    peer_row_dict["local_update_DTS"] = "null"
    peer_row_dict["execution_platform"] = os_platform
    peer_row_dict["python_version"] = python_version
    peer_row_dict["IPFS_agent"] = IPFS_agent
    peer_row_dict["agent"] = agent
    peer_row_dict["processing_status"] = "NPC"  # Normal peer processing complete
    peer_row_dict["disabled"] = "0"

    add_params = {"cid-version": 1, "only-hash": "false", "pin": "true"}
    with open(peer_file, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(peer_row_dict, write_file, indent=4)

    f = open(peer_file, "rb")
    add_files = {"file": f}
    response, status_code, response_dict = (
        execute_request(  # this is the true peer table row but not yet published
            url_key="add",
            # logger=logger,
            url_dict=url_dict,
            config_dict=config_dict,
            file=add_files,
            param=add_params,
            call_stack=call_stack,
        )
    )
    f.close()
    # TODO: 700 later
    insert_peer_row(conn, queries, peer_row_dict)
    conn.commit()

    object_CID = response_dict["Hash"]
    peer_row_CID = object_CID
    object_type = "local_peer_entry"

    mode = "init"
    processing_status = DTS

    header_CID = ipfs_header_add(
        call_stack,
        DTS,
        peer_row_CID,
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
    )

    print(f"Header containing the peer_row CID '{header_CID}'")
    DTS = get_DTS()

    network_table_dict = refresh_network_table_from_template()
    network_table_dict["network_name"] = import_car(call_stack)  # 3

    network_name = network_table_dict["network_name"]  # abused table dict entry
    insert_network_row(conn, queries, network_table_dict)
    conn.commit()

    object_CID = network_table_dict["network_name"]
    object_type = "network_name"

    mode = "init"
    processing_status = DTS
    header_CID = ipfs_header_add(
        call_stack,  # header for network name
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
    )

    add_shutdown_entry(
        conn,
        queries,
    )
    conn.commit()
    conn.close()
    return


def import_car(call_stack: str):
    call_stack = call_stack + ":import_car"
    url_dict = get_url_dict()
    db_init_config_dict = get_db_init_config_dict()
    # logger = get_logger(
    #    db_init_config_dict["log_file"],
    #    "none",
    # )

    car_path = get_car_path()
    dag_import_files = {"file": car_path}
    dag_import_params = {
        "pin-roots": "true",  # http status 500 if false but true does not pin if not in off-line mode ####name?
        "silent": "false",
        "stats": "false",
        "allow-big-block": "false",
    }

    response, status_code, response_dict = execute_request(
        url_key="dag_import",
        # logger=logger,
        url_dict=url_dict,
        config_dict=db_init_config_dict,
        file=dag_import_files,
        param=dag_import_params,
        call_stack=call_stack,
        http_500_ignore=False,
    )
    # TODO: 700 later
    imported_CID = response_dict["Root"]["Cid"]["/"]

    # import does not pin unless in offline mode so it must be done manually ###### name?
    pin_add_params = {"arg": imported_CID}

    response, status_code, response_dict = execute_request(
        url_key="pin_add",
        # logger=logger,
        url_dict=url_dict,
        config_dict=db_init_config_dict,
        file=dag_import_files,
        param=pin_add_params,
        call_stack=call_stack,
    )
    # TODO: 700 later
    return imported_CID


if __name__ == "__main__":
    init()
