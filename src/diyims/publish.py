def publish_main(call_stack: str, mode: str) -> None:
    from datetime import datetime
    from multiprocessing import freeze_support, set_start_method
    from multiprocessing.managers import BaseManager
    from queue import Empty
    from time import sleep

    from sqlalchemy.exc import NoResultFound
    from sqlmodel import Session, col, create_engine, select

    from diyims.config_utils import get_publish_config_dict
    from diyims.general_utils import get_DTS, set_controls, set_self, shutdown_query
    from diyims.logger_utils import add_log
    from diyims.path_utils import get_path_dict
    from diyims.requests_utils import execute_request
    from diyims.sqlmodels import Header_Table

    if __name__ != "__main__":
        freeze_support()
        try:
            set_start_method("spawn")
        except RuntimeError:
            pass

    call_stack = call_stack + ":publish_main"
    config_dict = get_publish_config_dict()

    path_dict = get_path_dict()
    SetControlsReturn = set_controls(call_stack, config_dict)
    SetSelfReturn = set_self()

    ipns_path = "/ipns/" + SetSelfReturn.IPNS_name

    wait_before_startup = int(config_dict["wait_before_startup"])
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Waiting for {wait_before_startup} seconds before startup.",
        )
    sleep(wait_before_startup)
    add_log(
        process=call_stack,
        peer_type="status",
        msg="Publish startup.",
    )

    if SetControlsReturn.queues_enabled and mode != "init":
        q_server_port = int(config_dict["q_server_port"])
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
        queue_server.register("get_publish_queue")
        queue_server.connect()
        in_bound = queue_server.get_publish_queue()

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
    status_code = 200

    while True:
        if shutdown_query(call_stack):
            break

        if SetControlsReturn.logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg="Entering resolve.",
            )
        start_DTS = get_DTS()
        #header_CID = most_recent_header.header_CID
        param = {"arg": ipns_path}
        _response, status_code, response_dict = execute_request(
            url_key="resolve",  # resolve last cid published by this peer for comparison to the latest header entry
            param=param,
            call_stack=call_stack,
            http_500_ignore=False,
        )
        if status_code == 200:
            if SetControlsReturn.logging_enabled:
                    stop_DTS = get_DTS()
                    start = datetime.fromisoformat(start_DTS)
                    stop = datetime.fromisoformat(stop_DTS)
                    duration = stop - start
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=f"Resolve completed in {duration} seconds with {status_code}.",
                    )
            resolved_header_CID = response_dict["Path"][6:]  # resolved format
        else:
            if SetControlsReturn.logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg="Resolve failed.",
                )

        if status_code == 200:
            statement = (
                select(Header_Table)
                .where(Header_Table.peer_ID == SetSelfReturn.self)
                .where(Header_Table.processing_status == "publish")
                .order_by(
                    col(Header_Table.origin_insert_DTS).asc()
                )  
            )

            header_list = []
            with Session(engine) as session:
                results = session.exec(statement)
                header_rows = results.all()

                for header in header_rows:
                    header_list.append(header)  # noqa: PERF402
            
            for header in header_list:
                
                header_CID = header.header_CID
                
                if shutdown_query(call_stack):
                    break
                
                if resolved_header_CID != header_CID:  # don't republish the same header
                    ipfs_path = "/ipfs/" + header_CID
                    if SetControlsReturn.logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg="Entering publish.",
                        )

                    name_publish_arg = {
                        "arg": ipfs_path,
                        "resolve": "true",
                        "key": "self",
                        "ipns-base": "base36",
                    }

                    start_DTS = get_DTS()
                    _response, status_code, response_dict = execute_request(
                        url_key="name_publish",
                        param=name_publish_arg,
                        call_stack=call_stack,
                        http_500_ignore=False,
                    )
                    if SetControlsReturn.logging_enabled:
                        stop_DTS = get_DTS()
                        start = datetime.fromisoformat(start_DTS)
                        stop = datetime.fromisoformat(stop_DTS)
                        duration = stop - start
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=f"Publish completed in {duration} seconds with {status_code}.",
                        )
                    if status_code != 200:
                        if SetControlsReturn.debug_enabled:
                            add_log(
                                process=call_stack,
                                peer_type="status",
                                msg=f"Publish {header_CID} failed with {status_code}.",
                            )
                        statement = ( # FIXME: factor into a header completion function
                            select(Header_Table).where(Header_Table.header_CID == header_CID)
                                )
                        DTS = get_DTS()
                        with Session(engine) as session:
                            try:
                                results = session.exec(statement)
                                header_row = results.one()
                                DTS = get_DTS()
                                header_row.processing_status = "publish failed"
                                header_row.processing_status_DTS = DTS
                                session.add(header_row)
                                session.commit()
                            except NoResultFound:
                                pass

                else:
                    statement = (
                                select(Header_Table).where(Header_Table.header_CID == header_CID)
                            )
                    DTS = get_DTS()
                    with Session(engine) as session:
                        try:
                            results = session.exec(statement)
                            header_row = results.one()
                            DTS = get_DTS()
                            header_row.processing_status = "completed"
                            header_row.processing_status_DTS = DTS
                            session.add(header_row)
                            session.commit()
                        except NoResultFound:
                            pass

            else:
                statement = (
                                select(Header_Table).where(Header_Table.header_CID == header_CID)
                            )
                DTS = get_DTS()
                with Session(engine) as session:
                    try:
                        results = session.exec(statement)
                        header_row = results.one()
                        DTS = get_DTS()
                        header_row.processing_status = "completed"
                        header_row.processing_status_DTS = DTS
                        session.add(header_row)
                        session.commit()
                    except NoResultFound:
                        pass
                if SetControlsReturn.logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=f"Header {header_CID} already published.",
                    )
        if shutdown_query(call_stack):
            break
        wait_for_next_request_seconds = int(config_dict["wait_time"])
        if SetControlsReturn.logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=f"Entering a wait of {wait_for_next_request_seconds}.",
            )
        #if most_recent_header is None:
        if SetControlsReturn.queues_enabled and mode != "init":
            try:
                in_bound.get(timeout=wait_for_next_request_seconds)
            except Empty:
                pass
        else:
            sleep(wait_for_next_request_seconds)

    add_log(
        process=call_stack,
        peer_type="status",
        msg=f"Publish complete {status_code}.",  # TODO:
    )


if __name__ == "__main__":
    import os
    from multiprocessing import freeze_support, set_start_method

    freeze_support()
    set_start_method("spawn")

    os.environ["DIYIMS_ROAMING"] = "Roaming"

    os.environ["QUEUES_ENABLED"] = "0"
    os.environ["LOGGING_ENABLED"] = "1"

    publish_main("__main__", "init")
