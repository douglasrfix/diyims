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

# from rich import print
# from diyims.database_utils import (
# insert_peer_row,
#    set_up_sql_operations,
# insert_header_row,
# refresh_peer_row_from_template,
# update_peer_table_status_to_PMP,
# update_peer_table_status_to_PMP_type_PR,
# add_header_chain_status_entry,
# )

# from diyims.database_utils import insert_header_row, set_up_sql_operations
# from multiprocessing.managers import BaseManager
# from diyims.requests_utils import execute_request
# from diyims.path_utils import get_path_dict, get_unique_file
# from diyims.logger_utils import add_log
# import json
# from sqlmodel import create_engine, Session, select
# from diyims.sqlmodels import Header_Table, Header_Chain_Status


def header_chain_maint(  # TODO needs to notify parallel functions based on additions  based upon type
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

    # from diyims.ipfs_utils import unpack_peer_row_from_cid
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
    # ipfs_config_dict = get_ipfs_config_dict()
    while True:
        start_DTS = get_DTS()
        # ipfs_sourced_header_CID =  "QmbY1Utuz753VwtQGyBvirB5Hgn8wouaRZ1xzBa99KaMRB"
        ipfs_path = "/ipfs/" + ipfs_sourced_header_CID

        param = {"arg": ipfs_path}
        response, status_code, response_dict = execute_request(
            url_key="cat",
            param=param,
            timeout=(3.05, 122),  # avoid timeouts
            call_stack=call_stack,
            http_500_ignore=False,
        )
        if status_code != 200:  # TODO: disable peer after ????
            new_header_chain_status = Header_Chain_Status(
                insert_DTS=get_DTS(),
                peer_ID=peer_ID,
                ipfs_header_CID=ipfs_sourced_header_CID,
                message="missing header",
            )

            with Session(engine) as session:
                try:
                    session.add(new_header_chain_status)
                    session.commit()

                # header_chain_status_dict = {}
                # header_chain_status_dict["insert_DTS"] = get_DTS()
                # header_chain_status_dict["peer_ID"] = peer_ID
                # header_chain_status_dict["missing_header_CID"] = ipfs_sourced_header_CID
                # header_chain_status_dict["message"] = "missing header"
                # conn, queries = set_up_sql_operations(config_dict)
                # try:
                #    add_header_chain_status_entry(
                #        conn,
                #        queries,
                #        header_chain_status_dict,
                #    )
                #    conn.commit()
                #    conn.close()
                except IntegrityError:
                    # conn.rollback()
                    # conn.close()
                    pass

            break  # log chain broken
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
            object_type = object_type
        except KeyError:
            msg = f"Invalid header: {ipfs_sourced_header_CID} from Peer: {peer_ID} ."
            add_log(
                process=call_stack,
                peer_type="Error",
                msg=msg,
            )
            break

        # object_CID = response_dict["object_CID"]
        """
        if object_type == "77local_peer_entry" or object_type == "77provider_peer_entry": #TODO: This will have to tolerate a partial peer entry and call the peer maintenance function
            status_code, remote_peer_row_dict = unpack_peer_row_from_cid(
                call_stack, object_CID, config_dict
            )
            # TODO: disable peer if != 200

            proto_remote_peer_row_dict = refresh_peer_row_from_template()
            proto_remote_peer_row_dict["peer_ID"] = remote_peer_row_dict["peer_ID"]
            proto_remote_peer_row_dict["peer_type"] = "RP"
            proto_remote_peer_row_dict["version"] = object_CID
            proto_remote_peer_row_dict["local_update_DTS"] = get_DTS()
            proto_remote_peer_row_dict["processing_status"] = "PMP"
            proto_remote_peer_row_dict["disabled"] = "0"

            conn, queries = set_up_sql_operations(config_dict)
            try:
                insert_peer_row(conn, queries, proto_remote_peer_row_dict)
                conn.commit()
                conn.close()
                if queues_enabled:
                    out_bound.put_nowait("wake up")
                    if debug_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg="Sent wakeup.",
                        )


                msg = f"Peer {remote_peer_row_dict['peer_ID']} added."
                if logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=msg,
                    )

            except IntegrityError:
                conn.rollback()
                conn.close()
                if object_type == "local_peer_entry":
                    # this will trigger peer maint by npp without change anything but the version, etc.
                    conn, queries = set_up_sql_operations(config_dict)
                    update_peer_table_status_to_PMP(
                        conn, queries, proto_remote_peer_row_dict
                    )
                    conn.commit()
                    conn.close()
                    if queues_enabled:
                        out_bound.put_nowait("wake up")
                        if debug_enabled:
                            add_log(
                                process=call_stack,
                                peer_type="status",
                                msg="Sent wakeup.",
                            )

                    msg = f"Peer {remote_peer_row_dict['peer_ID']} updated."
                    if logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=msg,
                        )
                elif object_type == "telemetry_entry":

                    pass

                else:
                    # peer_row_dict = select_peer_table_entry_by_key(conn, queries, remote_peer_row_dict)

                    # from PP to PR to indicate an overlay if not already NPP or NPC  triggers a provider source change
                    proto_remote_peer_row_dict["peer_type"] = "PR"
                    conn, queries = set_up_sql_operations(config_dict)
                    update_peer_table_status_to_PMP_type_PR(
                        conn, queries, proto_remote_peer_row_dict
                    )
                    conn.commit()
                    conn.close()
                    if queues_enabled:
                        out_bound.put_nowait("wake up")
                        if debug_enabled:
                            add_log(
                                process=call_stack,
                                peer_type="status",
                                msg="Sent wakeup.",
                            )

                    msg = f"Peer {remote_peer_row_dict['peer_ID']} updated."
                    if logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=msg,
                        )
        """
        # conn, queries = set_up_sql_operations(config_dict)
        new_header = Header_Table(
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
            try:  # this method adds one extra cat to the process
                session.add(new_header)
                session.commit()
            except IntegrityError:
                # pass

                # try:  # this method adds one extra cat to the process
                #    response_dict["processing_status"] = get_DTS()
                #    insert_header_row(conn, queries, response_dict, ipfs_sourced_header_CID)
                #    conn.commit()
                #    conn.close()

                # except IntegrityError:
                # pass will correct missing db components ########this leg shouldn't happen
                #    conn.rollback()
                #    conn.close()
                break

        # this method eliminates the cat  abd insert exception and uses a db read instead

        ipfs_sourced_header_CID = header_dict["prior_header_CID"]

        if ipfs_sourced_header_CID == "null":
            # header_chain_status_dict = {}
            # header_chain_status_dict["insert_DTS"] = get_DTS()
            # header_chain_status_dict["peer_ID"] = peer_ID
            # header_chain_status_dict["missing_header_CID"] = ipfs_sourced_header_CID
            # header_chain_status_dict["message"] = "Root header found"

            new_header_chain_status = Header_Chain_Status(
                insert_DTS=get_DTS(),
                peer_ID=peer_ID,
                ipfs_header_CID=ipfs_sourced_header_CID,
                message="Root header found",
            )

            with Session(engine) as session:
                try:
                    session.add(new_header_chain_status)
                    session.commit()
                except IntegrityError:
                    pass

            # conn, queries = set_up_sql_operations(config_dict)
            # add_header_chain_status_entry(
            #    conn,
            #    queries,
            #    header_chain_status_dict,
            # )
            # conn.commit()
            # conn.close()
            break  # log chain complete
        statement = (
            select(Header_Table)
            .where(Header_Table.peer_ID == peer_ID)
            .where(Header_Table.header_CID == ipfs_sourced_header_CID)
        )
        header_dict = {}
        with Session(engine) as session:
            results = session.exec(statement)
            if results.first() is None:
                pass
            else:
                break

        # conn, queries = set_up_sql_operations(config_dict)
        # db_header_row = queries.select_header_CID(  # TODO: change to db function
        #    conn, header_CID=ipfs_sourced_header_CID
        # )  # test for next entry
        # conn.close()
        # if (
        #    db_header_row is not None
        # ):  # If not missing, this will add to the chain until the prior is null
        #    break

    # a missing cid ot time out currently goes here
    # a nieve assumption would be to treat it as a missing cid

    # log gap
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
