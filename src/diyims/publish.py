def publish_main(call_stack: str, mode: str) -> None:
    from time import sleep
    from datetime import datetime
    from multiprocessing.managers import BaseManager
    from queue import Empty
    from multiprocessing import set_start_method, freeze_support
    from diyims.logger_utils import add_log
    from diyims.requests_utils import execute_request
    from diyims.config_utils import get_publish_config_dict
    from diyims.general_utils import shutdown_query, set_controls, set_self, get_DTS
    from sqlmodel import create_engine, Session, select, col
    from diyims.sqlmodels import Header_Table
    from diyims.path_utils import get_path_dict

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

    while True:
        if shutdown_query(call_stack):
            break
        statement_1 = (
            select(Header_Table)
            .where(Header_Table.peer_ID == SetSelfReturn.self)
            .order_by(col(Header_Table.insert_DTS).desc())
        )

        with Session(engine) as session:
            results = session.exec(statement_1)
            most_recent_header = results.first()

        if most_recent_header is None:
            pass
        else:
            if SetControlsReturn.logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg="Entering resolve.",
                )
            start_DTS = get_DTS()
            header_CID = most_recent_header.header_CID
            param = {"arg": ipns_path}
            response, status_code, response_dict = execute_request(
                url_key="resolve",  # resolve last cid published by this peer
                param=param,
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
                    msg=f"Resolve completed in {duration} seconds with {status_code}.",
                )
            if shutdown_query(call_stack):
                break
            if status_code == 200:
                ipfs_header_CID = response_dict["Path"][6:]  # resolved format
                if ipfs_header_CID != header_CID:  # don't republish the same header
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
                    }  # TODO: resolve duration as well as publish duration

                    start_DTS = get_DTS()
                    response, status_code, response_dict = execute_request(
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

                else:
                    if SetControlsReturn.debug_enabled:
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
        msg=f"Publish complete {status_code}.",
    )
    return


if __name__ == "__main__":
    import os
    from multiprocessing import set_start_method, freeze_support

    freeze_support()
    set_start_method("spawn")

    os.environ["DIYIMS_ROAMING"] = "RoamingDev"

    os.environ["QUEUES_ENABLED"] = "0"
    os.environ["LOGGING_ENABLED"] = "1"

    publish_main("__main__", "init")
