from pathlib import Path

from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from diyims.general_utils import get_DTS
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
    call_stack, logging_enabled, engine, config_dict, header_dict, new_header_CID, self
):
    call_stack = call_stack + ":peer_manager"
    update = False

    object_type = header_dict["object_type"]
    object_CID = header_dict["object_CID"]
    peer_ID = header_dict["peer_ID"]

    # if (
    #    object_type == "local_peer_entry"
    #    or object_type == "provider_peer_entry"
    #    or object_type == "remote_peer_entry"
    # ):
    status_code, peer_verified, remote_peer_row_dict = verify_peer_row_from_cid(
        call_stack,
        object_CID,
    )
    # FIXME: address handling should be factored into separate module
    if peer_verified:
        statement = select(Peer_Table).where(
            Peer_Table.peer_ID == remote_peer_row_dict["peer_ID"]
        )

        with Session(engine) as session:
            try:
                results = session.exec(statement)
                peer_row = results.one()
                peer_found = True
            except NoResultFound:
                peer_found = False

        if remote_peer_row_dict["peer_ID"] == self:
            provider_peer_ID = peer_ID

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

        statement = (  # check for the previous header in the db
            select(Header_Table).where(Header_Table.header_CID == new_header_CID)
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
            except NoResultFound:
                pass
        return status_code

    if peer_found:
        new_origin_value = remote_peer_row_dict[
            "origin_update_DTS"
        ]  # potential new values
        if peer_row.origin_update_DTS is None:
            current_origin_value = "0"  # There maybe nulls in legacy values
        else:
            current_origin_value = peer_row.origin_update_DTS

        if current_origin_value < new_origin_value:
            pass  # continue with update
        else:

            statement = (  # check for the previous header in the db
                        select(Header_Table).where(Header_Table.header_CID == new_header_CID)
                    )
            
            with Session(engine) as session:
                try:
                    results = session.exec(statement)
                    header_row = results.one()
                    DTS = get_DTS()
                    header_row.processing_status = "Peer Table update out of sequence"
                    header_row.processing_status_DTS = DTS
                    session.add(header_row)
                    session.commit()
                except NoResultFound:
                    pass
            return status_code

        if peer_row.peer_ID != self:
            peer_row.peer_ID = remote_peer_row_dict["peer_ID"]
            peer_row.IPNS_name = remote_peer_row_dict["IPNS_name"]
            peer_row.id = remote_peer_row_dict["id"]
            peer_row.signature = remote_peer_row_dict["signature"]
            peer_row.signature_valid = remote_peer_row_dict["signature_valid"]
            peer_row.origin_update_DTS = remote_peer_row_dict["origin_update_DTS"]
            peer_row.local_update_DTS = get_DTS()
            peer_row.execution_platform = remote_peer_row_dict["execution_platform"]
            peer_row.python_version = remote_peer_row_dict["python_version"]
            peer_row.IPFS_agent = remote_peer_row_dict["IPFS_agent"]
            peer_row.agent = remote_peer_row_dict["agent"]
            peer_row.version = remote_peer_row_dict["version"]
            peer_row.disabled = remote_peer_row_dict["disabled"]

            if object_type == "local_peer_entry":
                # this will trigger peer maint by npp without change anything but the version, etc.
                if peer_row.peer_type == "PP" and peer_row.processing_status != "NPC":
                    peer_row.processing_status = (
                        "NPC"  # update from WLR, WLRX, WLW, WLWX
                    )
                    peer_row.peer_type = (
                        "PR"  # update from PP since that process is incomplete
                    )

                update = True
            elif object_type == "provider_peer_entry" or object_type == "remote_peer_entry":
                if peer_row.peer_type == "PP" and peer_row.processing_status != "NPC":
                    peer_row.processing_status = (
                        "NPC"  # update from WLR, WLRX, WLW, WLWX
                    )
                    peer_row.peer_type = (
                        "PR"  # update from PP since that process is incomplete
                    )
                # peer_row.peer_type = "RP"
                update = True

        else:

            statement = (  # check for the previous header in the db
                                    select(Header_Table).where(Header_Table.header_CID == new_header_CID)
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
        # first time for the peer means its not a PP since that is local so it should ge in as RP meaning not a PP to start

        peer_row = Peer_Table(
            peer_ID=remote_peer_row_dict["peer_ID"],
            IPNS_name=remote_peer_row_dict["IPNS_name"],
            id=remote_peer_row_dict["id"],
            signature=remote_peer_row_dict["signature"],
            signature_valid=remote_peer_row_dict["signature_valid"],
            peer_type="RP",  # TODO: verify peer type logic
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
        update = True

    # else:
    #    pass

    if update:
        session.add(peer_row)
        session.commit()
        if peer_found:
            msg = f"Peer {remote_peer_row_dict['peer_ID']} updated."
            if logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=msg,
                )
        else:
            msg = f"Peer {remote_peer_row_dict['peer_ID']} added."
            if logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type="status",
                    msg=msg,
                )
        statement = (
                                select(Header_Table).where(Header_Table.header_CID == new_header_CID)
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
        # out_bound.put_nowait("wake up")
    return


def telemetry_manager(
    call_stack, logging_enabled, engine, config_dict, header_dict, new_header_CID, self
):
    call_stack = call_stack + ":telemetry_manager"
    object_CID = header_dict["object_CID"]

    _status_code, object_dict = unpack_object_from_cid(call_stack, object_CID)

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
            if telemetry_row.update_DTS <= object_dict["update_DTS"]:
                telemetry_row.peer_ID = object_dict["peer_ID"]
                telemetry_row.insert_DTS = object_dict["insert_DTS"]
                telemetry_row.update_DTS = object_dict["update_DTS"]
                telemetry_row.execution_platform = object_dict["execution_platform"]
                telemetry_row.python_version = object_dict["python_version"]
                telemetry_row.IPFS_agent = object_dict["IPFS_agent"]
                telemetry_row.DIYIMS_agent = object_dict["DIYIMS_agent"]
                #session.add(telemetry_row)
                #session.commit()

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
            select(Header_Table).where(Header_Table.header_CID == new_header_CID)
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



def update_manager(
    call_stack, logging_enabled, engine, config_dict, header_dict, new_header_CID, self
):
    call_stack = call_stack + ":update_manager"
    # call_stack = "test"

    # header_dict = {}
    # header_dict["object_CID"] = "bafkreiccz6lm2hb4kzcmmrnv6ffjrllrjsg7upubw54kryojdbzzs65m6i"

    meta_CID = header_dict["object_CID"]
    # msg = f'meta_CID from header {header_dict["object_CID"]}.'
    # add_log(
    #    process=call_stack,
    #    peer_type="status",
    #    msg=msg,
    # )
    # print("HeaderCID",  object_CID,)

    status_code, meta_dict = unpack_object_from_cid(call_stack, meta_CID)
    # msg = f'meta_dict {meta_dict}.'
    # add_log(
    #    process=call_stack,
    #    peer_type="status",
    #    msg=msg,
    # )
    # print("HeaderDict", status_code, object_dict)

    whl_CID = meta_dict["object_CID"]
    meta_data_dict = meta_dict["meta_data"]

    whl_name = meta_data_dict["file_name"]
    # msg = f'whl_cid from meta_dict {meta_dict["object_CID"]}.'
    # add_log(
    #    process=call_stack,
    #    peer_type="status",
    #    msg=msg,
    # )
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
            select(Header_Table).where(Header_Table.header_CID == new_header_CID)
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
    call_stack, logging_enabled, engine, config_dict, header_dict, new_header_CID, self
):
    call_stack = call_stack + ":generic_manager"
    msg = "Generic manager entered ."
    add_log(
        process=call_stack,
        peer_type="info",
        msg=msg,
    )

    meta_CID = header_dict["object_CID"]
    status_code, meta_dict = unpack_object_from_cid(call_stack, meta_CID)
    generic_CID = meta_dict["object_CID"]
    generic_data_dict = meta_dict["meta_data"]
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
            meta_CID=meta_CID,  # prior meta cid is ignored for now
        )

        with Session(engine) as session:
            session.add(new_meta)
            session.commit()

    msg = f'object_cid from meta_dict {meta_dict["object_CID"]}.'
    add_log(
        process=call_stack,
        peer_type="info",
        msg=msg,
    )
    # print("MetaCID", status_code, meta_CID)

    # status_code, meta_dict = unpack_object_from_cid(call_stack, meta_CID)
    # print("MetaDict", status_code, meta_dict)

    # ipfs_sourced_header_CID = "QmYYqUt8DfpHWjjJ9hVGFfnCVd3zrbHwgTom3jmZBVmxA8"
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
    # print(outfile)
    _response, status_code, _response_dictionary = execute_request(
        url_key="cat",
        param=param,
        timeout=(3.05, 122),  # avoid timeouts
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
    # print(status_code)
    if status_code == 200:
        msg = f"Using headerCID {new_header_CID} to update header."
        add_log(
            process=call_stack,
            peer_type="info",
            msg=msg,
        )
        statement = (
            select(Header_Table).where(Header_Table.header_CID == new_header_CID)
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



if __name__ == "__main__":
    update_manager()
