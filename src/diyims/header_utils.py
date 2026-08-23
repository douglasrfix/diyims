# from rich import print
from datetime import datetime

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, col, create_engine, select

from diyims.general_utils import get_DTS
from diyims.inbound_event_managers import (
    generic_manager,
    peer_manager,
    telemetry_manager,
    update_manager,
)
from diyims.logger_utils import add_log
from diyims.path_utils import get_path_dict
from diyims.requests_utils import execute_request
from diyims.sqlmodels import (
    Header_Chain_Status,
    Header_Table,
)


def header_chain_maint(
    call_stack,
    resolved_header_CID,
    config_dict,
    out_bound,
    peer_ID,  # will never be self
    logging_enabled,
    queues_enabled,
    debug_enabled,
    self,
):
    """
    docstring
    """
    call_stack = call_stack + ":header_chain_maint"
    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
    status_code = 200
    new_header_CID = resolved_header_CID

    while True:
        start_DTS = get_DTS()
        ipfs_path = "/ipfs/" + new_header_CID
        param = {"arg": ipfs_path}
        _response, status_code, response_dict = execute_request(
            url_key="cat",
            param=param,
            timeout=(3.05, 122),  # avoid timeouts
            call_stack=call_stack,
            http_500_ignore=False,
        )
        if status_code != 200:  # cat couldn't find the published header
            new_header_chain_status = Header_Chain_Status(
                insert_DTS=get_DTS(),
                peer_ID=peer_ID,
                missing_header_CID=new_header_CID,
                message="missing header",
            )

            with Session(engine) as session:
                try:
                    session.add(new_header_chain_status)
                    session.commit()

                except IntegrityError:
                    pass  # ignore duplicate message error could be restart

            break  # log chain broken so report and move on

        header_dict = response_dict
        stop_DTS = get_DTS()
        start = datetime.fromisoformat(start_DTS)
        stop = datetime.fromisoformat(stop_DTS)
        duration = stop - start
        msg = f"In {duration} CAT {header_dict}."
        if logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=msg,
            )
        try:
            object_type = header_dict["object_type"]
            #object_type = object_type  # avoiding editor errors
        except KeyError:
            msg = f"Invalid header format: {new_header_CID} from Peer: {peer_ID} ."
            add_log(
                process=call_stack,
                peer_type="Error",
                msg=msg,
            )
            break  # the dictionary doesn't contain the object type so isn't a valid header object
        DTS = get_DTS()
        try:
            origin_insert_DTS = header_dict["origin_insert_DTS"]
        except KeyError:
            origin_insert_DTS = "" 
        new_header = Header_Table(  # capture the published header
            version=header_dict["version"],
            object_CID=header_dict["object_CID"],
            object_type=header_dict["object_type"],
            insert_DTS=DTS,
            origin_insert_DTS=origin_insert_DTS,
            peer_ID=header_dict["peer_ID"],
            processing_status="added",
            processing_status_DTS= "",
            prior_header_CID=header_dict[
                "prior_header_CID"
            ],  # will contain "null" if this is the chain header
            header_CID=new_header_CID,
        )

        with Session(engine) as session:
            try:
                session.add(new_header)
                session.commit()
            except IntegrityError:
                pass

        if (
            object_type == "local_peer_entry"
            or object_type == "provider_peer_entry"
            or object_type == "remote_peer_entry"
        ):  # process peer entry or new header
            peer_manager(
                call_stack,
                logging_enabled,
                engine,
                config_dict,
                header_dict,
                new_header_CID,
                self,
            )
        elif object_type == "telemetry_entry":
            msg = "Telemetry entry submitted ."
            add_log(
                process=call_stack,
                peer_type="info",
                msg=msg,
            )
            telemetry_manager(
                call_stack,
                logging_enabled,
                engine,
                config_dict,
                header_dict,
                new_header_CID,
                self,
            )
        elif object_type == "whl_meta":
            msg = "Update entry submitted ."
            add_log(
                process=call_stack,
                peer_type="Error",
                msg=msg,
            )
            update_manager(
                call_stack,
                logging_enabled,
                engine,
                config_dict,
                header_dict,
                new_header_CID,
                self,
            )
        elif object_type == "generic_meta":
            msg = "Generic entry submitted ."
            add_log(
                process=call_stack,
                peer_type="info",
                msg=msg,
            )
            generic_manager(
                call_stack,
                logging_enabled,
                engine,
                config_dict,
                header_dict,
                new_header_CID,
                self,
            )
        else:
            msg = f"{object_type} entry Ignored ."
            add_log(
                process=call_stack,
                peer_type="info",
                msg=msg,
            )
        next_header_CID = header_dict["prior_header_CID"]

        if next_header_CID == "null":
            new_header_chain_status = Header_Chain_Status(
                insert_DTS=get_DTS(),
                peer_ID=peer_ID,
                missing_header_CID="na",
                message="Root header found",
            )

            with Session(engine) as session:
                try:
                    session.add(new_header_chain_status)
                    session.commit()
                except IntegrityError:
                    pass
            break  # header chain complete

        statement = (  # check for the previous header in the db
            select(Header_Table)
            .where(Header_Table.peer_ID == peer_ID)
            .where(Header_Table.header_CID == next_header_CID)
        )

        with Session(engine) as session:
            results = session.exec(statement)
            if results.first() is None:
                pass  # got to top of loop and cat prior header
            else:
                break  # need to exit since the prior header is in the db

    return status_code


def ipfs_header_add(
    call_stack,
    DTS,
    object_CID,
    object_type,
    peer_ID,
    config_dict,
    mode,
    processing_status,
    #processing_status_DTS,
    queues_enabled,
):
    import json
    from multiprocessing.managers import BaseManager

    from diyims.path_utils import get_path_dict, get_unique_file

    path_dict = get_path_dict()
    call_stack = call_stack + ":ipfs_header_add"
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
    if queues_enabled:
        q_server_port = int(config_dict["q_server_port"])
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
        queue_server.register(
            "get_publish_queue"
        )  # NOTE: eventually pass which queue to use
        queue_server.connect()
        publish_queue = queue_server.get_publish_queue()

    statement = (
        select(Header_Table)
        .where(Header_Table.peer_ID == peer_ID)
        # .where(Header_Table.object_type == "local_peer_entry")
        .order_by(col(Header_Table.insert_DTS).desc()) # newest header for the peerID
    )
    header_dict = {}
    with Session(engine) as session:
        results = session.exec(statement)
        header_row = results.first()
        if header_row is None:
            header_dict["prior_header_CID"] = "null"
        else:
            header_dict["prior_header_CID"] = header_row.header_CID

    header_dict["version"] = "0"
    header_dict["object_CID"] = object_CID
    header_dict["object_type"] = object_type
    header_dict["insert_DTS"] = DTS
    header_dict["origin_insert_DTS"] = DTS
    header_dict["peer_ID"] = peer_ID
    header_dict["processing_status"] = processing_status
    header_dict["processing_status_DTS"] = ""
    proto_path = path_dict["header_path"]
    proto_file = path_dict["header_file"]
    proto_file_path = get_unique_file(proto_path, proto_file)

    param = {"cid-version": 1, "only-hash": "false", "pin": "true", "pin-name": mode}

    with open(proto_file_path, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(header_dict, write_file, indent=4)

    with open(proto_file_path, "rb") as f:
        add_file = {"file": f}
        _response, status_code, response_dict = execute_request(
            url_key="add",
            param=param,
            file=add_file,
            call_stack=call_stack,
            http_500_ignore=False,
        )
    if status_code == 200:
        header_CID = response_dict["Hash"]
    else:
        add_log(
            process=call_stack,
            peer_type="Error",
            msg="IPFS Header Panic.",
        )
        return status_code, header_CID

    new_header = Header_Table(
        version=header_dict["version"],
        object_CID=header_dict["object_CID"],
        object_type=header_dict["object_type"],
        insert_DTS=header_dict["insert_DTS"],
        origin_insert_DTS=header_dict["origin_insert_DTS"],
        peer_ID=header_dict["peer_ID"],
        processing_status=header_dict["processing_status"],
        processing_status_DTS=header_dict["processing_status_DTS"],
        prior_header_CID=header_dict["prior_header_CID"],
        header_CID=header_CID,
    )

    with Session(engine) as session:
        session.add(new_header)
        session.commit()

    if queues_enabled:
        publish_queue.put_nowait("wake up")

    return status_code, header_CID
