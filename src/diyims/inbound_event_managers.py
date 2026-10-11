from pathlib import Path

#from sqlite3 import IntegrityError
#from rich import print
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlmodel import Session, select

from diyims.class_imports import SetControlsReturn, SetSelfReturn
from diyims.general_utils import get_DTS
from diyims.header_utils import ipfs_header_add
from diyims.ipfs_utils import unpack_object_from_cid
from diyims.logger_utils import add_log
from diyims.path_utils import get_path_dict
from diyims.requests_utils import execute_request
from diyims.security_utils import verify_peer_row_from_cid
from diyims.sqlmodels import (
    Header_Table,
    Object_Meta_Data,
    Peer_Address,
    Peer_Table,
    Peer_Telemetry,
)


def peer_manager(
    call_stack, set_self_return: SetSelfReturn, set_controls_return: SetControlsReturn, engine, config_dict, out_bound, header, peer_ID):
    """
    peer_manager maintains an image of the remote peer's Peer_Table

    _extended_summary_

    Arguments:
        call_stack {_type_} -- _description_
        set_self_return {SetSelfReturn} -- _description_
        set_controls_return {SetControlsReturn} -- _description_
        engine {_type_} -- _description_
        config_dict {_type_} -- _description_
        header {_type_} -- _description_

    Returns:
        _type_ -- _description_
    """

    """
    Peer_Table entries are created in one of three ways:

        1. A record is created during the installation process, of the type LP which means 'Local Provider',
            which means it is a provider of the network name CID. The network name provides the
            the target for the ipfs findprovs command. This identifies the local peer node.

        2. A record is created by the findprovs command. These are of the PP type which means 'Provider Provider'.

        3. A record is also created by this process if a 'Remote Provider' object type is processed.


    The peer manager process handles processing of header records and the updating of peer records.


    """
    
    
    call_stack = call_stack + ":peer_manager"
    update_peer_table = False
    update_header_table = False
    create_header = False
    # Before any type of peer record is processed the header is verified
    # All reference to local and remote are relative to the peer_ID passed via the function arguments 
    
    # if header.object type = local_peer_entry was created on the local system via the installation process
    # and the object_cid points to the peer table of the remote peer. 

    # if header.object type = provider_peer_entry was created on the local system via the findprovs and 
    # want list process and the object_CID points to the peer_table on the remote system
    # When the status of the peer_table entry becomes "NCP" the peer will be available to the monitoring peer publishing process.
    # The remote system may have been processing the local peer, if so this entry is ignored.

    # Since each peer is executing its own findprovs process, there will be times when both
    # the local and remote systems will be working on the same peer_ID and the systems are
    # processing the other systems publishing. In this case the arrival of a remote system provider_peer_entry
    # my arrive before the local system has finished processing the peer. In this case
    # the remote system provider entry will over ride
    # the local system and update the active peer table entry stat us to NCP.
    # This event is recorded by a remote_peer_entry in the header table and published.
    # This is mostly for metrics purposes.

    # if header.object_type = remote_peer_entry it is treated the same as a provider_peer_entry 

     

    
       
    status_code, peer_verified, remote_peer_row_dict = verify_peer_row_from_cid(
        call_stack,
        header.object_CID,
        peer_ID,
    )
    # TODO: address handling should be factored into separate module
    if peer_verified:
        statement = select(Peer_Table).where(
            Peer_Table.peer_ID == remote_peer_row_dict["peer_ID"]
        )

        with Session(engine) as session:
            try:
                results = session.exec(statement)
                peer_row = results.one()
                peer_found = True
                
                
                # if header.object type = remote peer the entry was created on the remote system via the peer manager process(this process)
            except NoResultFound:
                peer_found = False
        # If this condition is true, then the remote system has "discovered" the local system and created a valid peer table entry
        # and published a provider peer entry
        if remote_peer_row_dict["peer_ID"] == set_self_return.self: 
            provider_peer_ID = header.peer_ID
            # This checks for an active in use flag which can now be cleaned up since the discovery process is complete 
            statement = (
                select(Peer_Address)
                .where(Peer_Address.peer_ID == provider_peer_ID)
                .where(Peer_Address.in_use == "1")
            )

            with Session(engine) as session:
                try:
                    results = session.exec(statement).one()
                    address = results
                    peer_connected = True
                except NoResultFound:
                    peer_connected = False

            if peer_connected:
                provider_address = address.multiaddress

                param = {
                    "arg": provider_peer_ID,
                }
                _response, status_code, _response_dict = execute_request(
                    url_key="peering_remove",
                    param=param,
                    call_stack=call_stack,
                )

                if status_code == 200:
                    peering_removed = True

                param = {
                    "arg": provider_address,
                }

                _response, status_code, _response_dict = execute_request(
                    url_key="dis_connect",
                    param=param,
                    call_stack=call_stack,
                )

                if status_code == 200:
                    disconnected = True

                with Session(engine) as session:
                    address.in_use = False
                    if peering_removed:
                        address.peering_remove_DTS = get_DTS()
                    if disconnected:
                        address.dis_connect_DTS = get_DTS()
                    session.add(address)
                    session.commit()
                    session.refresh(address)

            # add_log(
            #    process=call_stack,
            #    peer_type="Status",
            #    msg=f"Disconnect point for peer {provider_peer_ID}",
            # )

    else:
        # do this after address handling
        statement = (
            
            select(Header_Table).where(Header_Table.header_CID == header.object_CID)
        )

        with Session(engine) as session:
            try:
                results = session.exec(statement)
                header_row = results.one()
                DTS = get_DTS()
                header_row.processing_status = "Peer not verified"
                header_row.processing_status_DTS = DTS
                session.add(header_row)
                session.commit()
            except NoResultFound: # shouldn't happen
                pass
        return status_code

    if peer_found:
        if peer_ID != set_self_return.self:

                    
            # A remote entry for a peer table was received and there is a current entry for
            # the peer where the want list processing is not complete
            
            if header.object_type == "local_peer_entry": # This would be the original peer table entry for the peer_ID
                
                
                # dont generate a header for a pear update. If it is already present there would have been a header for the original insert
                # make it a clone for each entry to reflect the remote peer data except for peer_type
                peer_row.peer_ID = remote_peer_row_dict["peer_ID"]
                peer_row.IPNS_name = remote_peer_row_dict["IPNS_name"]
                peer_row.id = remote_peer_row_dict["id"]
                peer_row.signature = remote_peer_row_dict["signature"]
                peer_row.signature_valid = remote_peer_row_dict["signature_valid"]
                peer_row.origin_update_DTS = remote_peer_row_dict["origin_update_DTS"]
                peer_row.local_update_DTS = get_DTS()
                peer_row.processing_status = "NPC"
                peer_row.processing_status = "PR"
                peer_row.execution_platform = remote_peer_row_dict["execution_platform"]
                peer_row.python_version = remote_peer_row_dict["python_version"]
                peer_row.IPFS_agent = remote_peer_row_dict["IPFS_agent"]
                peer_row.agent = remote_peer_row_dict["agent"]
                peer_row.version = remote_peer_row_dict["version"]
                peer_row.disabled = remote_peer_row_dict["disabled"]
        

                update_peer_table = True
                update_header_table = True
                create_header = False

                # this was generated by the remote peer due to remotes entry from  another peer.
                # we should wait for the original lp entry from the peer itself
            elif header.object_type == "provider_peer_entry":
            #elif header.object_type == "provider_peer_entry" or header.object_type == "remote_peer_entry":
                if peer_row.peer_type == "PP" and peer_row.processing_status != "NPC": #won't impact the local LP entry
                    #TODO: should generate a RP header since the local hasn't verified it yet
                    peer_row.processing_status = (
                        "NPC"  # update from WLR, WLRX, WLW, WLWX
                    )
                    peer_row.peer_type = (
                        "PR"  # update from PP since it the source is remote since that process is incomplete
                    )
                    update_peer_table = True
                    update_header_table = True
                    create_header = True
                else:    
                    update_peer_table = False
                    update_header_table = True
                    create_header = False
                

            elif header.object_type == "remote_peer_entry":
            #elif header.object_type == "provider_peer_entry" or header.object_type == "remote_peer_entry":
                if peer_row.peer_type == "PP" and peer_row.processing_status != "NPC":
                    #TODO: shout generate a RP header since the local hasn't verified it yet same as above
                    peer_row.processing_status = (
                        "NPC"  # update from WLR, WLRX, WLW, WLWX
                    )
                    peer_row.peer_type = (
                        "PR"  # update from PP since the source is remote to also remote to this peer since that process is incomplete
                    )
                    update_peer_table = True
                    update_header_table = True
                    create_header = True
                else:    
                    update_peer_table = False
                    update_header_table = True
                    create_header = False
                

            else: # This won't happen

                statement = (
                                        #select(Header_Table).where(Header_Table.header_CID == new_header_CID)
                                        select(Header_Table).where(Header_Table.header_CID == header.header_CID)
                                    )
                            
                with Session(engine) as session:
                    try:
                        results = session.exec(statement)
                        header_row = results.one()
                        DTS = get_DTS()
                        header_row.processing_status = "Peer Table has self as peerID"
                        header_row.processing_status_DTS = DTS
                        session.add(header_row)
                        session.commit()
                    except NoResultFound:
                        pass
                    return

        if not peer_found:
            # first time for the peer means its not a PP since that is local so it should ge in as RP meaning Remote peer to start
            # this can be caused by any of the types ???? 

            peer_row = Peer_Table(
                peer_ID=remote_peer_row_dict["peer_ID"],
                IPNS_name=remote_peer_row_dict["IPNS_name"],
                id=remote_peer_row_dict["id"],
                signature=remote_peer_row_dict["signature"],
                signature_valid=remote_peer_row_dict["signature_valid"],
                peer_type="RP",
                origin_update_DTS=remote_peer_row_dict["origin_update_DTS"],
                local_update_DTS=get_DTS(),
                execution_platform=remote_peer_row_dict["execution_platform"],
                python_version=remote_peer_row_dict["python_version"],
                IPFS_agent=remote_peer_row_dict["IPFS_agent"],
                processing_status="NPC",
                agent=remote_peer_row_dict["agent"],
                version=remote_peer_row_dict["version"],
                disabled=remote_peer_row_dict["disabled"],
            )
            update_peer_table = True
            update_header_table = True
            create_header = True

        
    if update_peer_table:
        session.add(peer_row)
        session.commit()
        if peer_found:
            msg = f"Peer {remote_peer_row_dict['peer_ID']} updated."
            if set_controls_return.logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=msg,
                )
        else:
            msg = f"Peer {remote_peer_row_dict['peer_ID']} added."
            if set_controls_return.logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=msg,
                )
    
    if update_header_table:
        statement = (
                                select(Header_Table).where(Header_Table.header_CID == header.header_CID)
                                #select(Header_Table).where(Header_Table.header_CID == new_header_CID)
                            )
                    
        with Session(engine) as session:
            try:
                results = session.exec(statement)
                header_row = results.one()
                DTS = get_DTS()
                header_row.processing_status = "completed-processing"
                header_row.processing_status_DTS = DTS
                session.add(header_row)
                session.commit()
            except NoResultFound:
                pass

    if create_header:
        # for an existing peer table entry
        #    update the current peer table entry to make it valid probably just a clone of the remote peer row
        #    when the local peer entry for the peer is processed, it will be set to the correct values
            
        #    the object_CID for the header is sourced from the current header object_CID
        #    the object type is a remote peer entry because it was the entry tha was completed by the remote system wantlist process based on the header.object_type
        
        object_type = "remote_peer_entry"  # comes from want list processing one entry per peer row
        # else:
        #    object_type = "remote_peer_entry"  # comes from peer monitor processing one entry per peer row

       
        # peer_ID = SetSelfReturn.self
        processing_status = "publish"
        #processing_status_DTS = DTS

        status_code, _header_CID = ipfs_header_add(
            call_stack,
            DTS,
            header.object_CID,
            object_type,
            set_self_return.self,
            config_dict,
            object_type,
            processing_status,
            #processing_status_DTS,
            set_controls_return.queues_enabled,
        )

    
    if set_controls_return.queues_enabled:
        out_bound.put_nowait("wake up")# send to publish 
    return


def telemetry_manager(
    call_stack, set_self_return: SetSelfReturn, set_controls_return: SetControlsReturn, engine, config_dict, out_bound, header,
):
    call_stack = call_stack + ":telemetry_manager"

    _status_code, object_dict = unpack_object_from_cid(call_stack, header.object_CID)
    # in this case the object is the telemetry file not a meta record#TODO: add meta record

    statement = select(Peer_Telemetry).where(
        Peer_Telemetry.peer_ID == object_dict["peer_ID"]
    )

    with Session(engine) as session:
        try:
            results = session.exec(statement)
            telemetry_row = results.one()
            found = True
        except NoResultFound:
            found = False

        if found:
            
            telemetry_row.peer_ID = object_dict["peer_ID"]
            telemetry_row.insert_DTS = object_dict["insert_DTS"]
            telemetry_row.update_DTS = object_dict["update_DTS"]
            telemetry_row.execution_platform = object_dict["execution_platform"]
            telemetry_row.python_version = object_dict["python_version"]
            telemetry_row.IPFS_agent = object_dict["IPFS_agent"]
            telemetry_row.DIYIMS_agent = object_dict["DIYIMS_agent"]
            
        else:
            telemetry_row = Peer_Telemetry(
                peer_ID=object_dict["peer_ID"],
                insert_DTS=object_dict["insert_DTS"],
                update_DTS=object_dict["update_DTS"],
                execution_platform=object_dict["execution_platform"],
                python_version=object_dict["python_version"],
                IPFS_agent=object_dict["IPFS_agent"],
                DIYIMS_agent=object_dict["DIYIMS_agent"],
            )
        session.add(telemetry_row)
        session.commit()

    
        statement = (
            
            select(Header_Table).where(Header_Table.header_CID == header.header_CID)
        )
        
        with Session(engine) as session:
            try:
                results = session.exec(statement)
                header_row = results.one()
                DTS = get_DTS()
                header_row.processing_status = "completed-processing"
                header_row.processing_status_DTS = DTS
                session.add(header_row)
                session.commit()
                
            except NoResultFound:
                
                pass



def update_manager(
    call_stack, set_self_return: SetSelfReturn, set_controls_return: SetControlsReturn, engine, config_dict, header,
):
    call_stack = call_stack + ":update_manager"
    # call_stack = "test"

    # header_dict = {}
    # header_dict["object_CID"] = "bafkreiccz6lm2hb4kzcmmrnv6ffjrllrjsg7upubw54kryojdbzzs65m6i"

    #meta_CID = header_dict["object_CID"]
    meta_CID = header.object_CID
    #msg = f'meta_CID from header {header_dict["object_CID"]}.'
    #add_log(
    #   process=call_stack,
    #   peer_type="status",
    #   msg=msg,
    #)
    #print("HeaderCID",  object_CID,)

    status_code, meta_dict = unpack_object_from_cid(call_stack, meta_CID)
    #msg = f'meta_dict {meta_dict}.'
    #add_log(
    #   process=call_stack,
    #   peer_type="status",
    #   msg=msg,
    #.)
    # print("HeaderDict", status_code, object_dict)

    whl_CID = meta_dict["object_CID"]
    status_code, meta_meta_dict = unpack_object_from_cid(call_stack, whl_CID)

    
    meta_data_dict = meta_meta_dict["meta_data"]

    whl_name = meta_data_dict["file_name"]
    msg = f'whl_cid from meta_dict {meta_dict["object_CID"]}.'
    add_log(
       process=call_stack,
       peer_type="status",
       msg=msg,
    )
    # print("MetaCID", status_code, meta_CID)

    # status_code, meta_dict = unpack_object_from_cid(call_stack, meta_CID)
    # print("MetaDict", status_code, meta_dict)

    # ipfs_sourced_header_CID = "QmYYqUt8DfpHWjjJ9hVGFfnCVd3zrbHwgTom3jmZBVmxA8"
    path_dictionary = get_path_dict()
    ipfs_path = "/ipfs/" + str(whl_CID)
    param = {"arg": ipfs_path}
    outfile = str(path_dictionary["update_path"]) + "\\" + whl_name
    # print(outfile)
    _response, status_code, _response_dictionary = execute_request(
        url_key="cat",
        param=param,
        timeout=(3.05, 122),  # avoid timeouts
        call_stack=call_stack,
        http_500_ignore=False,
        stream=True,
        outfile=outfile,  # TODO: user interface to support entry of the file name with a predefined path
    )
    # TODO: Support publishing as well as retrieval into standard library

    # print(status_code)
    if status_code == "200":
        statement = (
            #select(Header_Table).where(Header_Table.header_CID == new_header_CID)
            select(Header_Table).where(Header_Table.header_CID == header.header_CID)
        )

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


def generic_manager(
    call_stack, set_self_return: SetSelfReturn, set_controls_return: SetControlsReturn, engine, config_dict, header,
):
    call_stack = call_stack + ":generic_manager"
    msg = "Generic manager entered ."
    add_log(
        process=call_stack,
        peer_type="info",
        msg=msg,
    )

    # header.object_CID = meta object CID 
    status_code, meta_dict = unpack_object_from_cid(call_stack, header.object_CID)
    generic_CID = meta_dict["object_CID"]
    try:
        generic_data_dict = meta_dict["meta_data"] # test for supplementary meta_data dictionary #TODO: expand to be version and test that first
    except KeyError:
        statement = (
                    select(Header_Table).where(Header_Table.header_CID == header.object_CID)
                )
        DTS = get_DTS()
        with Session(engine) as session:
            try:
                results = session.exec(statement)
                header_row = results.one()
                DTS = get_DTS()
                header_row.processing_status = "meta data failed"
                header_row.processing_status_DTS = DTS
                session.add(header_row)
                session.commit()
            except NoResultFound: # should never happen since we are processing a header
                pass
        status_code = 1
        return status_code
        
    work_path = Path(generic_data_dict["file_name"])
    file_name = work_path.name
    file_type = work_path.suffix
    DTS = get_DTS()

    if file_type == ".whl":
        pass
    else:
        new_meta = Object_Meta_Data(
            version=meta_dict["version"],
            object_CID=meta_dict["object_CID"],
            object_type=meta_dict["object_type"],
            peer_ID=meta_dict["peer_ID"],
            insert_DTS=DTS,
            meta_data=meta_dict["meta_data"],
            meta_CID=header.object_CID,  # prior meta cid is ignored for now
        )

        with Session(engine) as session:
            try:
                session.add(new_meta)
                session.commit()
            except IntegrityError:
                pass #TODO: expand the logic to handle error conditions such as temporary objects
                        # check for existing files etc. replace older Files?

    msg = f'object_cid from meta_dict {meta_dict["object_CID"]}.'
    add_log(
        process=call_stack,
        peer_type="info",
        msg=msg,
    )
    
    path_dictionary = get_path_dict()
    ipfs_path = "/ipfs/" + str(generic_CID)
    param = {"arg": ipfs_path}

    msg = f"ipfs path {ipfs_path} for CAT."
    add_log(
        process=call_stack,
        peer_type="info",
        msg=msg,
    )
    if file_type == ".whl":
        outfile = Path(path_dictionary["update_path"]).joinpath(file_name)
    else:
        outfile = Path(path_dictionary["generic_path"]).joinpath(file_name)
    
    _response, status_code, _response_dictionary = execute_request(
        url_key="cat",
        param=param,
        timeout=(3.05, 122),  # avoid timeouts #TODO:may need to be rethought
        call_stack=call_stack,
        http_500_ignore=False,
        stream=True,
        outfile=outfile,
    )
    msg = f"CAT for {generic_CID} completed with stats code {status_code}."
    add_log(
        process=call_stack,
        peer_type="info",
        msg=msg,
    )
    
    if status_code == 200:
        msg = f"Using headerCID {header.header_CID} to update header."
        add_log(
            process=call_stack,
            peer_type="info",
            msg=msg,
        )
        statement = (
            select(Header_Table).where(Header_Table.header_CID == header.header_CID)
        )
        DTS = get_DTS()
        with Session(engine) as session:
            try:
                results = session.exec(statement)
                header_row = results.one()
                DTS = get_DTS()
                header_row.processing_status = "completed-processing"
                header_row.processing_status_DTS = DTS
                session.add(header_row)
                session.commit()
            except NoResultFound:
                pass #shouldn't happen

