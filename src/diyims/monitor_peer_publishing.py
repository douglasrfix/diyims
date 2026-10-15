
from datetime import datetime
from multiprocessing import freeze_support, set_start_method
from multiprocessing.managers import BaseManager
from queue import Empty
from time import sleep

#from rich import print
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, col, create_engine, select

from diyims.class_imports import SetControlsReturn, SetSelfReturn
from diyims.config_utils import get_peer_monitor_config_dict
from diyims.general_utils import get_DTS, set_controls, set_self, shutdown_query
from diyims.header_utils import header_chain_maint
from diyims.inbound_event_managers import (
    generic_manager,
    peer_manager,
    telemetry_manager,
    #update_manager,
)
from diyims.logger_utils import add_log
from diyims.path_utils import get_path_dict
from diyims.requests_utils import execute_request
from diyims.sqlmodels import Header_Chain_Status, Header_Table, Peer_Table


def monitor_peer_publishing_main(call_stack: str) -> None:
    """
    This process is responsible for the capture of objects published
    by other members of the network.

    Periodically, an IPFS resolve is issued to each active peer to discover
    the latest CID published.

    If an unknown CID has been published the CID is captured and processed. Otherwise
    the CID is ignored.

    
     

    """
    if __name__ != "__main__":
        freeze_support()
        try:
            set_start_method("spawn")
        except RuntimeError:
            pass
    call_stack = call_stack + ":monitor_peer_publishing"
    config_dict = get_peer_monitor_config_dict()
    set_controls_return = set_controls(call_stack, config_dict)
    set_self_return = set_self()

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    if set_controls_return.queues_enabled:
            q_server_port = int(config_dict["q_server_port"])
            queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
            queue_server.register("get_publish_queue")
            queue_server.register("get_remote_monitor_queue")
            queue_server.connect()
            out_bound =  queue_server.get_publish_queue()
            in_bound = queue_server.get_remote_monitor_queue()
    else:
        out_bound = None

    wait_time = int(config_dict["wait_time"])
    wait_before_startup = int(config_dict["wait_before_startup"])
    if set_controls_return.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Waiting for {wait_before_startup} seconds before startup.",
        )
    sleep(wait_before_startup)
    
    add_log(
        process=call_stack,
        peer_type="status",
        msg="Remote Peer Monitor startup.",
    )
    status_code = 200
    while True:
        
        peer_list = create_peer_list(call_stack, set_self_return, set_controls_return, engine)
        

        for peer in peer_list:
            if shutdown_query(call_stack):
                break

            
            _response, status_code, response_dict = resolve_peer(call_stack, set_self_return, set_controls_return, peer)
            if status_code != 200:
                
                process_failed_resolve(call_stack, set_self_return, set_controls_return, peer, engine)
                
            else:
                #peer_ID = peer.peer_ID
                # from resolve above that determines wether the peer is active and what the most current published cid is
                resolved_header_CID = response_dict["Path"][6:]
                # header cid in a published object which is always a header in this system
                
                    

                #for header in header_list: # header(s) to be processed by owning peer insert date
                # how do we process the unrecorded new entry???? #TODO: how about same entry as last time?????
                start_DTS = get_DTS()
                 # add one or more headers
                header_status_code = header_chain_maint(
                    call_stack,
                    set_self_return.self,
                    set_controls_return,
                    resolved_header_CID,
                    config_dict,
                    out_bound,
                    peer.peer_ID,  # will never be self
                    
                ) 
                if set_controls_return.logging_enabled:
                    stop_DTS = get_DTS()
                    start = datetime.fromisoformat(start_DTS)
                    stop = datetime.fromisoformat(stop_DTS)
                    duration = stop - start
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=f"header_chain_maint for {peer.peer_ID} completed in {duration} seconds with {header_status_code}.",
                    )
                if header_status_code == 1: # header maint failed
                    process_failed_header_maint(call_stack, set_self_return, set_controls_return, engine, header_status_code, resolved_header_CID)

                else:

                    #create header list here #TODO: add check for chain completion before processing entries to minimize out of order potential due to processing failures  
                    statement = (  # look for existing headers that haven,t been processed
                        select(Header_Table.version,
                               Header_Table.object_CID,
                               Header_Table.object_type,
                               Header_Table.insert_DTS,
                               Header_Table.origin_insert_DTS,
                               Header_Table.prior_header_CID,
                               Header_Table.header_CID,
                               Header_Table.peer_ID,
                               Header_Table.processing_status,
                               Header_Table.processing_status_DTS,
                               Header_Chain_Status)
                        .where(Header_Table.peer_ID == peer.peer_ID)
                        #.where(Header_Chain_Status.peer_ID == peer.peer_ID)
                        .where(Header_Table.processing_status == "add-requested")
                        .where(Header_Table.peer_ID == Header_Chain_Status.peer_ID)
                        .order_by(
                            col(Header_Table.origin_insert_DTS).asc()
                        )
                    )
                    header_list = []
                    with Session(engine) as session: #gather preexisting headers if any
                        results = session.exec(statement)
                        header_rows = results.all()
            
                    for header in header_rows:
                        header_list.append(header)# noqa: PERF402
                    
                    process_inbound_events_for_peer(call_stack, set_self_return,
                                        set_controls_return, engine, config_dict, out_bound,  header_list, peer.peer_ID)
                    

                    
                # header list for peer completed
                # # out_bound.put_nowait("wake up")
                    
                # out_bound.put_nowait("wake up")
                
        try:  # peer list completed
            if set_controls_return.queues_enabled:
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

def create_peer_list(call_stack, set_self_return, set_controls_return, engine) -> list:
    statement_1 = (
        select(Peer_Table)
        .where(Peer_Table.signature_valid == 1)
        .where(Peer_Table.peer_ID != set_self_return.self) #ignore the local peer
        .where(Peer_Table.processing_status == "NPC") #only peers that are ready for normal processing
        )
    peer_list = []
    with Session(engine) as session:
                results = session.exec(statement_1)
                peer_rows = results.all()
                for peer in peer_rows:
                    peer_list.append(peer)  # noqa: PERF402
    
    return peer_list

def resolve_peer(call_stack, set_self_return, set_controls_return, peer):
    #if (  # select peer to process. ignore the local peer and select only peers that have been processed
    #    peer.processing_status == "NPC"
    #):  # this single threads updates from a  remote peer
    ipns_path = "/ipns/" + peer.IPNS_name

    start_DTS = get_DTS()
    param = {"arg": ipns_path}
    response, status_code, response_dict = (
        execute_request(  # This returns the most recent object cid published by the peer
            url_key="resolve",
            param=param,
            call_stack=call_stack,
            connect_retries=0,
            http_500_ignore=False,
        )
    )
    if set_controls_return.logging_enabled:
        stop_DTS = get_DTS()
        start = datetime.fromisoformat(start_DTS)
        stop = datetime.fromisoformat(stop_DTS)
        duration = stop - start
        add_log(
            process=call_stack,
            peer_type="status",
            msg=f"Resolve completed in {duration} seconds with {status_code}.",
        )
    return response, status_code, response_dict

def process_failed_resolve(call_stack, set_self_return: SetSelfReturn, set_controls_return: SetControlsReturn, peer, engine):
    statement_2 = select(Peer_Table).where(
        Peer_Table.peer_ID == peer.peer_ID
    )
    with Session(engine) as session:
        results = session.exec(statement_2)
        current_peer = results.one()
        current_peer.disabled = 0  # disable peer on first resolve failure #TODO: fix this or maybe put a request on the network for the most recent from this peer?
        session.add(current_peer)
        session.commit()
    if set_controls_return.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="Error",
            msg=f"Resolve for {peer.peer_ID} failed and peer disabled.",
        )
    
def process_failed_header_maint(call_stack, set_self_return: SetSelfReturn, set_controls_return: SetControlsReturn, engine, header_status_code, resolved_header_CID):
    statement = (
        select(Header_Table).where(Header_Table.header_CID == resolved_header_CID)
    )
    DTS = get_DTS()
    with Session(engine) as session:
        
        results = session.exec(statement)
        header_row = results.one()
        DTS = get_DTS()
        header_row.processing_status = "failed"
        header_row.processing_status_DTS = DTS
        session.add(header_row)
        session.commit()
        
    if set_controls_return.logging_enabled:
        add_log(
            process=call_stack,
            peer_type="Error",
            msg=f"Remote Monitor Panic {header_status_code}.",
        )
        
def process_inbound_events_for_peer(call_stack, set_self_return: SetSelfReturn,
                    set_controls_return: SetControlsReturn, engine, config_dict, out_bound, header_list: list, peer_ID):
    
    for header in header_list:
        if (
            header.object_type == "local_peer_entry"
            or header.object_type == "provider_peer_entry"
            or header.object_type == "remote_peer_entry"
        ):  # process peer entry or new header
            peer_manager(
                call_stack,
                set_self_return,
                set_controls_return,
                engine,
                config_dict,
                out_bound,
                header,
                peer_ID,
                
            )
        elif header.object_type == "telemetry_entry":
            msg = "Telemetry entry submitted ."
            add_log(
                process=call_stack,
                peer_type="info",
                msg=msg,
            )
            telemetry_manager(
                call_stack,
                set_self_return,
                set_controls_return,
                engine,
                config_dict,
                out_bound,
                header,
                #new_header_CID,
                
            )
        elif header.object_type == "whl_meta_":
            msg = "Update entry submitted ."
            add_log(
                process=call_stack,
                peer_type="Error",
                msg=msg,
            )
            _status_code = generic_manager(
                call_stack,
                set_self_return,
                set_controls_return,
                engine,
                config_dict,
                header,
                #new_header_CID,
                
            )
            #if status_code == 1:
            #    break
        elif header.object_type == "generic_meta":
            msg = "Generic entry submitted ."
            add_log(
                process=call_stack,
                peer_type="info",
                msg=msg,
            )
            _status_code = generic_manager(
                call_stack,
                set_self_return,
                set_controls_return,
                engine,
                config_dict,
                out_bound,
                header,
                #new_header_CID,
                
            )
            #if status_code == 1:
            #    break
        else:
            msg = f"{header.object_type} entry Ignored ."
            add_log(
                process=call_stack,
                peer_type="info",
                msg=msg,
            )
            statement = (
                            select(Header_Table).where(Header_Table.header_CID == header.header_CID)
                        )
                                    
            with Session(engine) as session:
                try:
                    results = session.exec(statement)
                    header_row = results.one()
                    DTS = get_DTS()
                    header_row.processing_status = "Ignored"
                    header_row.processing_status_DTS = DTS
                    session.add(header_row)
                    session.commit()
                except NoResultFound:
                    pass
            
    

if __name__ == "__main__":
    import os
    from multiprocessing import freeze_support, set_start_method

    freeze_support()
    set_start_method("spawn")

    os.environ["COMPONENT_TEST"] = "0"
    os.environ["QUEUES_ENABLED"] = "0"
    monitor_peer_publishing_main("__main__")
