
def sync_ipfs_agent_table():
    from sqlmodel import Session, create_engine, delete

    from diyims.path_utils import get_path_dict
    from diyims.sqlmodels import IPFS_Agents

    # Tested Versions
    kubo_0431 = IPFS_Agents(agent_id="kubo/0.43.1")
    kubo_0430 = IPFS_Agents(agent_id="kubo/0.43.0")
    kubo_0420 = IPFS_Agents(agent_id="kubo/0.42.0")
    kubo_0410 = IPFS_Agents(agent_id="kubo/0.41.0")
    kubo_0400 = IPFS_Agents(agent_id="kubo/0.40.0")
    kubo_0390 = IPFS_Agents(agent_id="kubo/0.39.0")
    kubo_0370 = IPFS_Agents(agent_id="kubo/0.37.0")
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"
    engine = create_engine(db_url, echo=False, connect_args={"timeout": 10})

    # statement = select(IPFS_Agents)
    statement = delete(IPFS_Agents)

    with Session(engine) as session:
        session.exec(statement)
        session.commit()

    with Session(engine) as session:
        session.add(kubo_0431)
        session.add(kubo_0430)
        session.add(kubo_0420)
        session.add(kubo_0410)
        session.add(kubo_0400)
        session.add(kubo_0390)
        session.add(kubo_0370)
        session.commit()

    #return


def reset_peer_table_status(call_stack):
    # from diyims.config_utils import get_want_list_config_dict
    # from diyims.py_version_dep import get_sql_str
    #from sqlite3 import SQLITE_NOTFOUND

    
    from sqlmodel import Session, create_engine, or_, select

    from diyims.path_utils import get_path_dict
    from diyims.sqlmodels import Peer_Table, Shutdown

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


def refresh_network_table_from_template():
    network_table_dict = {}
    network_table_dict["version"] = "0"
    network_table_dict["network_name"] = "null"
    return network_table_dict


if __name__ == "__main__":
    reset_peer_table_status("__main__")
