def reset_peer_table_status(call_stack):
    # from diyims.config_utils import get_want_list_config_dict
    from diyims.path_utils import get_path_dict
    # from diyims.py_version_dep import get_sql_str

    # import sqlite3
    from sqlmodel import create_engine, Session, select, or_
    from diyims.sqlmodels import Shutdown, Peer_Table, Peer_Address
    from sqlalchemy.exc import NoResultFound
    from diyims.general_utils import get_DTS

    # config_dict = get_want_list_config_dict()
    call_stack = call_stack + ":reset_peer_table_status"
    path_dict = get_path_dict()
    # sql_str = get_sql_str()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"

    engine = create_engine(db_url, echo=False, connect_args={"timeout": 120})

    # conn = sqlite3.connect(connect_path, timeout=int(config_dict["sql_timeout"]))
    # conn = sqlite3.connect(connect_path, timeout=int(600))
    # conn.row_factory = sqlite3.Row

    # update peer_table set processing_status = "WLR"
    # where processing_status  = "WLRX" or processing_status = "WLP"

    statement = select(Peer_Table).where(
        or_(
            Peer_Table.processing_status == "WLRX",
            Peer_Table.processing_status == "WLWX",
            Peer_Table.processing_status == "WLRP",
            Peer_Table.processing_status == "WLWP",
        )
    )
    with Session(engine) as session:
        results = session.exec(statement).all()
        for peer in results:
            if peer.processing_status == "WLRX" or peer.processing_status == "WLRP":
                peer.processing_status = "WLR"
            if peer.processing_status == "WLWX" or peer.processing_status == "WLWP":
                peer.processing_status = "WLW"

            session.add(peer)
        session.commit()

    statement = select(Shutdown)
    with Session(engine) as session:
        results = session.exec(statement).all()
        for row in results:
            row.enabled = 0

            session.add(row)
        session.commit()

        """
        Reconnect at startup before regular processing
        1 Examine peer addresses for in use.
        2 Examine peer table for processing status NCP

        if an address is in use but the peer is NCP then the reverse connect is in effect

        need a in use that is an interim value
        """

        statement = select(Peer_Address).where(
            Peer_Address.in_use == 1
        )  # TODO handle reconnect after restart for reverse connect
        with Session(engine) as session:
            results = session.exec(statement).all()
            addresses = results

        for address in addresses:
            statement2 = select(Peer_Table).where(
                Peer_Table.processing_status == "NCP"
                and Peer_Table.peer_ID == address.peer_ID
            )
            with Session(engine) as session:
                try:
                    results = session.exec(statement2).one()
                    address.in_use = False
                    address.reset_DTS = get_DTS()
                    address.connect_DTS = None
                    address.peering_add_DTS = None
                    with Session(engine) as session:
                        session.add(address)
                        session.commit()
                    status_code, peer_connected = peer_connect(
                        call_stack, address.peer_ID
                    )

                except NoResultFound:
                    address.in_use = False
                    address.reset_DTS = get_DTS()
                    address.connect_DTS = None
                    address.peering_add_DTS = None
                    with Session(engine) as session:
                        session.add(address)
                        session.commit()
    return


def peer_connect(
    call_stack: str,
    peer_ID: str,
    # SetControlsReturn: SetControlsReturn,
) -> bool:
    from diyims.path_utils import get_path_dict
    from sqlmodel import create_engine, Session, select, col
    from diyims.sqlmodels import Peer_Address
    from sqlalchemy.exc import NoResultFound
    from diyims.requests_utils import execute_request
    from diyims.general_utils import get_DTS

    call_stack = call_stack + ":peer_connect"
    peer_connected = False
    status_code = 200
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"

    # engine = create_engine(db_url, echo=True)
    engine = create_engine(db_url, echo=False, connect_args={"timeout": 120})

    statement = (
        select(Peer_Address)
        .where(Peer_Address.peer_ID == peer_ID)
        .where(Peer_Address.in_use == "1")
    )

    with Session(engine) as session:
        try:
            results = session.exec(statement).one()
            peer_connected = True
            status_code = 200
            return status_code, peer_connected
        except NoResultFound:
            peer_connected = False

    if peer_connected:  # TODO: is this a good idea?
        pass

    else:
        statement = (
            select(Peer_Address)
            .where(Peer_Address.peer_ID == peer_ID)
            .where(Peer_Address.available == "1")
            .order_by(col(Peer_Address.insert_DTS).desc())
        )
        with Session(engine) as session:
            results = session.exec(statement).all()

        for peer_address in results:
            param = {"arg": peer_address.multiaddress}

            response, status_code, response_dict = execute_request(
                url_key="connect",
                param=param,
                call_stack=call_stack,
            )
            # if successfully connected update address used
            if status_code == 200:
                peer_connected = True
                statement = select(Peer_Address).where(
                    Peer_Address.address_string == peer_address.address_string
                )
                with Session(engine) as session:
                    results = session.exec(statement)
                    address = results.one()

                    address.in_use = True
                    address.connect_DTS = get_DTS()

                    session.add(address)
                    session.commit()

                response, status_code, response_dict = execute_request(
                    url_key="peering_add",
                    param=param,
                    call_stack=call_stack,
                )

                if status_code == 200:
                    statement = select(Peer_Address).where(
                        Peer_Address.address_string == peer_address.address_string
                    )
                    with Session(engine) as session:
                        results = session.exec(statement)
                        address = results.one()

                        address.peering_add_DTS = get_DTS()

                        session.add(address)
                        session.commit()

                else:
                    status_code = 200
                    # if SetControlsReturn.debug_enabled:
                    #    add_log(
                    #        process=call_stack,
                    #        peer_type="status",
                    #        msg=f"{peer_ID} peering add failed.",
                    #    )
            elif status_code == 500:
                peer_connected = False
            else:
                peer_connected = False
                # if SetControlsReturn.debug_enabled:
                #    add_log(
                #        process=call_stack,
                #        peer_type="status",
                #        msg=f"Peer connect failed for {peer_ID}.",
                #    )

            if peer_connected:
                break  # don't need any more connections

    return status_code, peer_connected


def refresh_network_table_from_template():
    network_table_dict = {}
    network_table_dict["version"] = "0"
    network_table_dict["network_name"] = "null"
    return network_table_dict


if __name__ == "__main__":
    reset_peer_table_status("__main__")
