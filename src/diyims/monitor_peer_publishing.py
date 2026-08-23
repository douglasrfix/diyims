import os
from datetime import datetime
from multiprocessing import freeze_support, set_start_method
from multiprocessing.managers import BaseManager
from queue import Empty
from time import sleep

from sqlmodel import Session, col, create_engine, select

from diyims.config_utils import get_peer_monitor_config_dict
from diyims.general_utils import get_DTS, set_controls, set_self, shutdown_query
from diyims.header_utils import header_chain_maint
from diyims.logger_utils import add_log
from diyims.path_utils import get_path_dict
from diyims.requests_utils import execute_request
from diyims.sqlmodels import Header_Table, Peer_Table

# from rich import print

# FIXME: if a header is added but not completed, there is currently no provision for recovery!!!!!!!!!!

def monitor_peer_publishing_main(call_stack: str) -> None:
    """
    docstring
    """
    if __name__ != "__main__":
        freeze_support()
        try:
            set_start_method("spawn")
        except RuntimeError:
            pass
    call_stack = call_stack + ":monitor_peer_publishing"
    config_dict = get_peer_monitor_config_dict()
    wait_time = int(config_dict["wait_time"])
    path_dict = get_path_dict()
    SetControlsReturn = set_controls(call_stack, config_dict)
    SetSelfReturn = set_self()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    wait_before_startup = int(config_dict["wait_before_startup"])
    if SetControlsReturn.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Waiting for {wait_before_startup} seconds before startup.",
        )
    sleep(wait_before_startup)
    # if SetControlsReturn.logging_enabled:
    add_log(
        process=call_stack,
        peer_type="status",
        msg="Remote Peer Monitor startup.",
    )

    status_code = 200

    if SetControlsReturn.queues_enabled:
        q_server_port = int(config_dict["q_server_port"])
        queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
        queue_server.register("get_peer_maint_queue")
        queue_server.register("get_remote_monitor_queue")
        queue_server.connect()
        out_bound = queue_server.get_peer_maint_queue()
        in_bound = queue_server.get_remote_monitor_queue()
    else:
        out_bound = None

    statement_1 = (
        select(Peer_Table)
        .where(Peer_Table.signature_valid == 1)
        .where(Peer_Table.peer_ID != SetSelfReturn.self)
    )

    while True:
        peer_list = []
        with Session(engine) as session:
            results = session.exec(statement_1)
            peer_rows = results.all()
            for peer in peer_rows:
                peer_list.append(peer)  # noqa: PERF402

        for peer in peer_list:
            if shutdown_query(call_stack):
                break

            if (  # select peer to process. ignore the local peer and select only peers that have been processed
                peer.processing_status == "NPC"
            ):  # this single threads updates from a  remote peer
                ipns_path = "/ipns/" + peer.IPNS_name

                start_DTS = get_DTS()
                param = {"arg": ipns_path}
                _response, status_code, response_dict = (
                    execute_request(  # This returns the most recent object cid published by the peer
                        url_key="resolve",
                        param=param,
                        call_stack=call_stack,
                        connect_retries=0,
                        http_500_ignore=False,
                    )
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

                if status_code != 200:
                    statement_2 = select(Peer_Table).where(
                        Peer_Table.peer_ID == peer.peer_ID
                    )
                    with Session(engine) as session:
                        results = session.exec(statement_2)
                        current_peer = results.one()
                        current_peer.disabled = 0  # disable peer on first resolve failure #TODO: fix this or maybe put a request on the network for the most recent from this peer?
                        session.add(current_peer)
                        session.commit()
                    if SetControlsReturn.logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="Error",
                            msg=f"Resolve for {peer.peer_ID} failed and peer disabled.",
                        )

                if status_code == 200:
                    peer_ID = peer.peer_ID
                    # from resolve above that determines wether the peer is active and what the most current published cid is
                    resolved_header_CID = response_dict["Path"][6:]
                    # header cid in a published object which is always a header in this system      #loop on header list!!!!!
                    statement = (  # look for an existing header
                        select(Header_Table)
                        .where(Header_Table.peer_ID == peer_ID)
                        .where(Header_Table.peer_ID != resolved_header_CID)
                        .where(Header_Table.processing_status == "added")
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
                        start_DTS = get_DTS()
                        
                        status_code = header_chain_maint(
                            call_stack,
                            resolved_header_CID,
                            config_dict,
                            out_bound,
                            peer_ID,  # will never be self
                            SetControlsReturn.logging_enabled,
                            SetControlsReturn.queues_enabled,
                            SetControlsReturn.debug_enabled,
                            SetSelfReturn.self,
                        )  # add one or more headers
                        if SetControlsReturn.logging_enabled:
                            stop_DTS = get_DTS()
                            start = datetime.fromisoformat(start_DTS)
                            stop = datetime.fromisoformat(stop_DTS)
                            duration = stop - start
                            add_log(
                                process=call_stack,
                                peer_type="status",
                                msg=f"header_chain_maint for {peer.peer_ID} completed in {duration} seconds with {status_code}.",
                            )
                        if status_code != 200:  # noqa: SIM102
                            if SetControlsReturn.logging_enabled:
                                add_log(
                                    process=call_stack,
                                    peer_type="Error",
                                    msg=f"Remote Monitor Panic {status_code}.",
                                )
                            # out_bound.put_nowait("wake up")
                       
                    # out_bound.put_nowait("wake up")
        try:  # peer list completed
            if SetControlsReturn.queues_enabled:
                in_bound.get(
                    timeout=wait_time  # config value
                )  # comes from peer capture process
                if shutdown_query(call_stack):
                    break

            else:
                sleep(wait_time)  # config value
                if shutdown_query(call_stack):
                    break

        except Empty:
            if shutdown_query(call_stack):
                break

    add_log(
        process=call_stack,
        peer_type="status",
        msg=f"Remote Peer Monitor complete with {status_code}.",
    )


if __name__ == "__main__":
    import os
    from multiprocessing import freeze_support, set_start_method

    freeze_support()
    set_start_method("spawn")

    os.environ["COMPONENT_TEST"] = "1"
    os.environ["QUEUES_ENABLED"] = "0"
    monitor_peer_publishing_main("__main__")
