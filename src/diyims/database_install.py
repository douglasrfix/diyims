import json
import os

from rich import print
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, SQLModel, create_engine, select, text

# from diyims.logger_utils import get_logger
from diyims.config_utils import get_db_init_config_dict
from diyims.error_classes import (
    ApplicationNotInstalledError,
    PreExistingInstallationError,
    UnSupportedIPFSVersionError,
)
from diyims.general_utils import SetControlsReturn, get_agent, get_DTS, verify_peerID
from diyims.header_utils import ipfs_header_add
from diyims.ipfs_utils import get_url_dict
from diyims.path_utils import get_path_dict, get_unique_file
from diyims.platform_utils import get_python_version, test_os_platform
from diyims.py_version_dep import get_car_path
from diyims.requests_utils import execute_request
from diyims.security_utils import sign_file, verify_file
from diyims.sqlmodels import Network_Table, Peer_Table, Peer_Telemetry, Shutdown

# from fastapi.encoders import jsonable_encoder


def create(call_stack):
    try:
        path_dict = get_path_dict()

    except ApplicationNotInstalledError:  # noqa: TRY203
        raise

    call_stack = call_stack + ":create"
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"
    # print(db_url)  TODO: log this and success

    engine = create_engine(db_url, echo=False, connect_args={"timeout": 10})
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session = Session(engine)
        statement = text("PRAGMA journal_mode = WAL;")
        session.exec(statement)
        print("DB Schema creation successful.")

    #return


def init(call_stack):
    call_stack = call_stack + ":init"
    try:
        path_dict = get_path_dict()

    except ApplicationNotInstalledError:  # noqa: TRY203
        raise

    url_dict = get_url_dict()
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"
    engine = create_engine(db_url, echo=False, connect_args={"timeout": 120})

    config_dict = get_db_init_config_dict()
    # logger = get_logger(
    #    config_dict["log_file"],
    #    "none",
    # )
    try:
        wait_dict = verify_peerID(call_stack)
    except UnSupportedIPFSVersionError:  # noqa: TRY203
        raise

    # TODO:use verify id below and order operations to minimize function calls

    statement = select(Network_Table)

    with Session(engine) as session:
        try:
            results = session.exec(statement)
            network_row = results.one()
            #network_row = network_row
            network_found = True
        except NoResultFound:
            network_found = False

    # conn, queries = set_up_sql_operations(config_dict)
    # Rconn, Rqueries = set_up_sql_operations(config_dict)

    # network_name = queries.select_network_name(conn)

    if network_found:
        raise (PreExistingInstallationError(" "))

    IPFS_agent = wait_dict["AgentVersion"]
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

    peer_ID = wait_dict["ID"]

    # TODO: 700 later
    # prepare a dictionary with the single value of the peer id extracted from with "id" command.
    signing_dict = {}
    signing_dict["peer_ID"] = peer_ID
    # prepare a file containing the signing dictionary.
    proto_path = path_dict["sign_path"]
    proto_file = path_dict["sign_file"]
    proto_file_path = get_unique_file(proto_path, proto_file)

    with open(proto_file_path, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(signing_dict, write_file, indent=4)

    # create a signature for file just created.
    # The id is the public key of the key pair used by the process
    # the signature is the string used to verify that the user/owner of the private signed the particular contents of the file
    sign_dict = {}
    sign_dict["file_to_sign"] = proto_file_path

    status_code, id, signature = sign_file(call_stack, sign_dict)

    # Present the file, public key(id)and the signature to thw verify function
    verify_dict = {}
    verify_dict["signed_file"] = proto_file_path
    verify_dict["id"] = id
    verify_dict["signature"] = signature

    status_code, signature_valid = verify_file(call_stack, verify_dict)
    # a signature valid means that the file was signed by the key pair using this public key.
    # In fact any file with the same contents will result in a positive result
    #
    # This fact allows a remote user to create the same contents by using the peerID, id and signature to

    """
    Create the initial peer table entry for this peer.
    """

    DTS = get_DTS()

    """
    given the peer_ID from what ever source, the id and the signature, the authenticity of the user of that id is verified.
    the id is the same as the ipns name for that peer id.

    """

    peer_row_partial = Peer_Table(
        peer_ID=peer_ID,
        # IPNS_name = IPNS_name,  # capture ipns_name # use the id?#TODO: eliminate extra publish
        id=id,
        signature=signature,
        signature_valid=signature_valid,
        peer_type="LP",  # local provider peer
        origin_update_DTS=DTS,
        local_update_DTS=DTS,
        execution_platform=os_platform,
        python_version=python_version,
        IPFS_agent=IPFS_agent,
        agent=agent,
        processing_status="NPC",  # Normal peer processing complete
        disabled=0,
    )

    peer_row_partial_dict = dict(peer_row_partial)
    # peer_row_partial_dict = jsonable_encoder(peer_row_partial)
    proto_path = path_dict["peer_path"]
    proto_file = path_dict["peer_file"]
    proto_file_path = get_unique_file(proto_path, proto_file)

    peer_file = proto_file_path

    add_params = {"cid-version": 1, "only-hash": "false", "pin": "false"}
    with open(peer_file, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(peer_row_partial_dict, write_file, indent=4)

    with open(peer_file, "rb") as f:
        add_files = {"file": f}
        _response, status_code, response_dict = (
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
    # f.close()
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

    _response, status_code, response_dict = execute_request(  # publish to get ipns name
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

    peer_row = Peer_Table(
        peer_ID=peer_ID,
        IPNS_name=IPNS_name,  # capture ipns_name
        id=id,
        signature=signature,
        signature_valid=signature_valid,
        peer_type="LP",  # local provider peer
        origin_update_DTS=DTS,
        local_update_DTS=DTS,
        execution_platform=os_platform,
        python_version=python_version,
        IPFS_agent=IPFS_agent,
        agent=agent,
        processing_status="NPC",  # Normal peer processing complete
        disabled=0,
    )

    peer_row_dict = dict(peer_row)
    # peer_row_dict = jsonable_encoder(peer_row)

    add_params = {
        "cid-version": 1,
        "only-hash": "false",
        "pin": "true",
        "pin-name": "init",
    }
    with open(peer_file, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(peer_row_dict, write_file, indent=4)

    with open(peer_file, "rb") as f:
        add_files = {"file": f}
        _response, status_code, response_dict = (
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
    # f.close()
    # TODO: 700 later
    with Session(engine) as session:
        session.add(peer_row)
        session.commit()

    # conn.commit()

    object_CID = response_dict["Hash"]
    peer_row_CID = object_CID
    object_type = "local_peer_entry"

    mode = "init"
    processing_status = "publish"
    #processing_status_DTS = DTS
    # FIXME: add meta data first the header pointing to meta
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
        #processing_status_DTS,
        SetControlsReturn.queues_enabled,
        # Rconn,
        # Rqueries,
    )

    print(f"Header containing the peer_row CID '{header_CID}'")
    DTS = get_DTS()

    network_table_dict = {}
    network_table_dict["network_name"] = import_car(call_stack)
    network_row = Network_Table(network_name=network_table_dict["network_name"])
    with Session(engine) as session:
        session.add(network_row)
        session.commit()

    # network_name = network_table_dict["network_name"]  # abused table dict entry
    # insert_network_row(conn, queries, network_table_dict)
    # conn.commit()

    object_CID = network_table_dict["network_name"]  # TODO: package in json
    object_type = "network_name"

    mode = "init"
    processing_status = "publish"
    #processing_status_DTS = DTS
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
        #processing_status_DTS,
        SetControlsReturn.queues_enabled,
        # Rconn,
        # Rqueries,
    )
    shutdown_row = Shutdown(enabled=0)
    with Session(engine) as session:
        session.add(shutdown_row)
        session.commit()

    telemetry_row = Peer_Telemetry(  # TODO: package in json and add header entry
        peer_ID=peer_ID,
        insert_DTS=DTS,
        update_DTS=DTS,
        execution_platform=os_platform,
        python_version=python_version,
        IPFS_agent=IPFS_agent,
        DIYIMS_agent=agent,
    )

    telemetry_dict = dict(telemetry_row)
    # telemetry_dict = jsonable_encoder(telemetry_row)

    with Session(engine) as session:
        session.add(telemetry_row)
        session.commit()
        session.refresh(telemetry_row)

    proto_file = path_dict["peer_file"]
    param = {
        "cid-version": 1,
        "only-hash": "false",
        "pin": "true",
        "pin-name": "update_telemetry",
    }
    with open(proto_file, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(telemetry_dict, write_file, indent=4)

    with open(proto_file, "rb") as f:
        add_file = {"file": f}
        _response, status_code, response_dict = execute_request(
            url_key="add",
            url_dict=url_dict,
            config_dict=config_dict,
            file=add_file,
            param=param,
            call_stack=call_stack,
            http_500_ignore=False,
        )
    # f.close()

    object_CID = response_dict["Hash"]  # new peer row cid
    object_type = "telemetry_entry"
    mode = object_type
    processing_status = "publish"
    #processing_status_DTS = DTS

    status_code = ipfs_header_add(
        call_stack,
        DTS,
        object_CID,
        object_type,
        peer_ID,
        config_dict,
        mode,
        processing_status,
        #processing_status_DTS,
        SetControlsReturn.queues_enabled,
    )

    #    conn,
    #    queries,
    # )
    # conn.commit()
    # conn.close()
    return status_code


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

    _response, _status_code, response_dict = execute_request(
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

    pin_remove_params = {"arg": imported_CID}

    _response, _status_code, response_dict = execute_request(
        url_key="pin_remove",
        # logger=logger,
        url_dict=url_dict,
        config_dict=db_init_config_dict,
        file=dag_import_files,
        param=pin_remove_params,
        call_stack=call_stack,
    )

    # import does not pin unless in offline mode so it must be done manually ###### name?
    pin_add_params = {"arg": imported_CID, "pin-name": "init"}

    _response, _status_code, response_dict = execute_request(
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
    init("__main__")
