# from rich import print


def header_chain_maint(
    call_stack,
    ipfs_sourced_header_CID,
    config_dict,
    out_bound,
    peer_ID,
    logging_enabled,
    queues_enabled,
    debug_enabled,
):
    from diyims.requests_utils import execute_request
    from diyims.logger_utils import add_log
    from diyims.general_utils import get_DTS
    from datetime import datetime
    from sqlite3 import IntegrityError
    from diyims.path_utils import get_path_dict
    from sqlmodel import create_engine, Session, select
    from diyims.sqlmodels import Header_Table, Header_Chain_Status

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

    while True:
        start_DTS = get_DTS()
        ipfs_path = "/ipfs/" + ipfs_sourced_header_CID
        param = {"arg": ipfs_path}
        response, status_code, response_dict = execute_request(
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
                missing_header_CID=ipfs_sourced_header_CID,
                message="missing header",
            )

            with Session(engine) as session:
                try:
                    session.add(new_header_chain_status)
                    session.commit()

                except IntegrityError:
                    pass  # ignore duplicate message error

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
            object_type = object_type  # avoiding editor errors
        except KeyError:
            msg = f"Invalid header: {ipfs_sourced_header_CID} from Peer: {peer_ID} ."
            add_log(
                process=call_stack,
                peer_type="Error",
                msg=msg,
            )
            break  # the dictionary doesn't contain the object type so isn't a valid header object

        new_header = Header_Table(  # capture the published header
            version=header_dict["version"],
            object_CID=header_dict["object_CID"],
            object_type=header_dict["object_type"],
            insert_DTS=get_DTS(),
            peer_ID=header_dict["peer_ID"],
            processing_status=get_DTS(),
            prior_header_CID=header_dict["prior_header_CID"],
            header_CID=ipfs_sourced_header_CID,
        )

        with Session(engine) as session:
            try:
                session.add(new_header)
                session.commit()
            except IntegrityError:
                break  # header already exists so we are done

        ipfs_sourced_header_CID = header_dict["prior_header_CID"]

        if ipfs_sourced_header_CID == "null":
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
            break  # log chain complete

        statement = (  # check for the previous header in the db
            select(Header_Table)
            .where(Header_Table.peer_ID == peer_ID)
            .where(Header_Table.header_CID == ipfs_sourced_header_CID)
        )

        with Session(engine) as session:
            results = session.exec(statement)
            if results.first() is None:
                pass  # got to top of loop and cat prior header
            else:
                break  # need to continue since the prior header is in the db

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
    queues_enabled,
):
    # from diyims.database_utils import insert_header_row, set_up_sql_operations
    from multiprocessing.managers import BaseManager
    from diyims.requests_utils import execute_request
    from diyims.path_utils import get_path_dict, get_unique_file
    from diyims.logger_utils import add_log
    import json
    from diyims.sqlmodels import Header_Table
    from sqlmodel import create_engine, Session, select, col
    from diyims.general_utils import get_DTS

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
        .where(Header_Table.object_type == "local_peer_entry")
        .order_by(col(Header_Table.insert_DTS).desc())
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
    header_dict["peer_ID"] = peer_ID
    header_dict["processing_status"] = processing_status

    proto_path = path_dict["header_path"]
    proto_file = path_dict["header_file"]
    proto_file_path = get_unique_file(proto_path, proto_file)

    param = {"cid-version": 1, "only-hash": "false", "pin": "true", "pin-name": mode}

    with open(proto_file_path, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(header_dict, write_file, indent=4)

    f = open(proto_file_path, "rb")
    add_file = {"file": f}
    response, status_code, response_dict = execute_request(
        url_key="add",
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

    new_header = Header_Table(
        version=header_dict["version"],
        object_CID=header_dict["object_CID"],
        object_type=header_dict["object_type"],
        insert_DTS=get_DTS(),
        peer_ID=header_dict["peer_ID"],
        processing_status=header_dict["processing_status"],
        prior_header_CID=header_dict["prior_header_CID"],
        header_CID=header_CID,
    )

    with Session(engine) as session:
        session.add(new_header)
        session.commit()

    if queues_enabled:
        publish_queue.put_nowait("wake up")

    return status_code, header_CID
