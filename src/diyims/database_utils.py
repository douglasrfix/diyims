# from os import close


def set_up_sql_operations(config_dict):  # TODO: row v s cursor set up ?
    from diyims.path_utils import get_path_dict
    from diyims.py_version_dep import get_sql_str
    import aiosql
    import sqlite3

    path_dict = get_path_dict()
    sql_str = get_sql_str()
    connect_path = path_dict["db_file"]
    queries = aiosql.from_str(sql_str, "sqlite3")
    conn = sqlite3.connect(connect_path, timeout=int(config_dict["sql_timeout"]))
    # conn = sqlite3.connect(connect_path, timeout=int(600))
    conn.row_factory = sqlite3.Row
    return conn, queries


def set_up_sql_operations_list(config_dict):
    from diyims.path_utils import get_path_dict
    from diyims.py_version_dep import get_sql_str
    import aiosql
    import sqlite3

    path_dict = get_path_dict()
    sql_str = get_sql_str()
    connect_path = path_dict["db_file"]
    queries = aiosql.from_str(sql_str, "sqlite3")
    conn = sqlite3.connect(connect_path, timeout=int(config_dict["sql_timeout"]))
    # conn = sqlite3.connect(connect_path, timeout=int(600))
    return conn, queries


def reset_peer_table_status():
    # from diyims.config_utils import get_want_list_config_dict
    from diyims.path_utils import get_path_dict
    from diyims.py_version_dep import get_sql_str
    import aiosql
    import sqlite3

    # config_dict = get_want_list_config_dict()
    path_dict = get_path_dict()
    sql_str = get_sql_str()
    connect_path = path_dict["db_file"]
    queries = aiosql.from_str(sql_str, "sqlite3")
    # conn = sqlite3.connect(connect_path, timeout=int(config_dict["sql_timeout"]))
    conn = sqlite3.connect(connect_path, timeout=int(600))
    conn.row_factory = sqlite3.Row

    queries.reset_peer_table_status(
        conn,
    )
    conn.commit()

    update_shutdown_enabled_0(conn, queries)
    conn.commit()
    conn.close
    return


def add_header_chain_status_entry(conn, queries, header_chain_status_dict):
    queries.add_header_chain_status_entry(
        conn,
        insert_DTS=header_chain_status_dict["insert_DTS"],
        peer_ID=header_chain_status_dict["peer_ID"],
        missing_header_CID=header_chain_status_dict["missing_header_CID"],
        message=header_chain_status_dict["message"],
    )
    return


def add_shutdown_entry(conn, queries):
    queries.add_shutdown_entry(
        conn,
    )
    return


def update_shutdown_enabled_1(conn, queries):
    queries.update_shutdown_enabled_1(
        conn,
    )
    return


def update_shutdown_enabled_0(conn, queries):
    queries.update_shutdown_enabled_0(
        conn,
    )
    return


def select_shutdown_entry(conn, queries):
    shutdown_row_dict = queries.select_shutdown_entry(
        conn,
    )
    return shutdown_row_dict


def insert_peer_row(conn, queries, peer_table_dict):
    queries.insert_peer_row(
        conn,
        peer_ID=peer_table_dict["peer_ID"],
        IPNS_name=peer_table_dict["IPNS_name"],
        id=peer_table_dict["id"],
        signature=peer_table_dict["signature"],
        signature_valid=peer_table_dict["signature_valid"],
        peer_type=peer_table_dict["peer_type"],
        origin_update_DTS=peer_table_dict["origin_update_DTS"],
        local_update_DTS=peer_table_dict["local_update_DTS"],
        execution_platform=peer_table_dict["execution_platform"],
        python_version=peer_table_dict["python_version"],
        IPFS_agent=peer_table_dict["IPFS_agent"],
        processing_status=peer_table_dict["processing_status"],
        agent=peer_table_dict["agent"],
        version=peer_table_dict["version"],
    )
    return


def update_peer_row_by_key_status(conn, queries, peer_row_dict):
    queries.update_peer_row_by_key_status(
        conn,
        IPNS_name=peer_row_dict["IPNS_name"],
        id=peer_row_dict["id"],
        signature=peer_row_dict["signature"],
        signature_valid=peer_row_dict["signature_valid"],
        peer_type="PP",
        origin_update_DTS=peer_row_dict["origin_update_DTS"],
        local_update_DTS=peer_row_dict["local_update_DTS"],
        execution_platform=peer_row_dict["execution_platform"],
        python_version=peer_row_dict["python_version"],
        IPFS_agent=peer_row_dict["IPFS_agent"],
        processing_status=peer_row_dict["processing_status"],
        agent=peer_row_dict["agent"],
        version=peer_row_dict["version"],
        peer_ID=peer_row_dict["peer_ID"],
    )
    return


def select_peer_table_entry_by_key(conn, queries, peer_table_dict):
    """_summary_

    Args:
        conn (_type_): _description_
        queries (_type_): _description_
        peer_table_dict (_type_): _description_

    Returns:
        _type_: _description_
    """
    peer_table_entry = queries.select_peer_table_entry_by_key(
        conn,
        peer_ID=peer_table_dict["peer_ID"],
    )
    return peer_table_entry


def select_peer_table_local_peer_entry(conn, queries, peer_table_dict):
    peer_table_entry = queries.select_peer_table_local_peer_entry(
        conn,
    )
    return peer_table_entry


def update_peer_table_version(conn, queries, peer_table_dict):
    queries.update_peer_table_version(
        conn,
        local_update_DTS=peer_table_dict["local_update_DTS"],
        version=peer_table_dict["version"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_peer_type_status(conn, queries, peer_table_dict):
    queries.update_peer_table_peer_type_status(
        conn,
        peer_type=peer_table_dict["peer_type"],
        processing_status=peer_table_dict["processing_status"],
        local_update_DTS=peer_table_dict["local_update_DTS"],
        version=peer_table_dict["version"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_peer_type_BP_to_PP(conn, queries, peer_table_dict):
    queries.update_peer_table_peer_type_BP_to_PP(
        conn,
        local_update_DTS=peer_table_dict["local_update_DTS"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_peer_type_SP_to_PP(conn, queries, peer_table_dict):
    queries.update_peer_table_peer_type_SP_to_PP(
        conn,
        local_update_DTS=peer_table_dict["local_update_DTS"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_status_WLR(conn, queries, peer_table_dict):
    queries.update_peer_table_status_WLR(
        conn,
        # processing_status=peer_table_dict["processing_status"],
        local_update_DTS=peer_table_dict["local_update_DTS"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_status_WPW_to_WLR(conn, queries, peer_table_dict):
    queries.update_peer_table_status_WPW_to_WLR(
        conn,
        # processing_status=peer_table_dict["processing_status"],
        local_update_DTS=peer_table_dict["local_update_DTS"],
        version=peer_table_dict["version"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_status_WLP(conn, queries, peer_table_dict):
    queries.update_peer_table_status_WLP(
        conn,
        # processing_status=peer_table_dict["processing_status"],
        local_update_DTS=peer_table_dict["local_update_DTS"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_status_WLX(conn, queries, peer_table_dict):
    queries.update_peer_table_status_WLX(
        conn,
        # processing_status=peer_table_dict["processing_status"],
        local_update_DTS=peer_table_dict["local_update_DTS"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_status_WLZ(conn, queries, peer_table_dict):
    queries.update_peer_table_status_WLZ(
        conn,
        # processing_status=peer_table_dict["processing_status"],
        local_update_DTS=peer_table_dict["local_update_DTS"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_status_to_NPP(conn, queries, peer_table_dict):
    """
    Summary:

    _summary_

    Updates IPNS_name, id, signature, signature_valid, origin_update_DTS, local_update_DTS,
            execution_platform, python_version, IPFS_agent, processing_status, and agent
        based upon peer_ID

    Args:
        conn (_type_): _description_
        queries (_type_): _description_
        peer_table_dict (_type_): _description_
    """
    queries.update_peer_table_status_to_NPP(
        conn,
        # peer_type=peer_table_dict["peer_type"],
        local_update_DTS=peer_table_dict["local_update_DTS"],
        version=peer_table_dict["version"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_status_to_PMP(conn, queries, peer_table_dict):
    """
    Summary:

    _summary_

    Updates IPNS_name, id, signature, signature_valid, origin_update_DTS, local_update_DTS,
            execution_platform, python_version, IPFS_agent, processing_status, and agent
        based upon peer_ID

    Args:
        conn (_type_): _description_
        queries (_type_): _description_
        peer_table_dict (_type_): _description_
    """
    queries.update_peer_table_status_to_PMP(
        conn,
        # peer_type=peer_table_dict["peer_type"],
        local_update_DTS=peer_table_dict["local_update_DTS"],
        version=peer_table_dict["version"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_status_to_NPP_type_PR(conn, queries, peer_table_dict):
    """
    Summary:

    _summary_

    Updates IPNS_name, id, signature, signature_valid, origin_update_DTS, local_update_DTS,
            execution_platform, python_version, IPFS_agent, processing_status, and agent
        based upon peer_ID

    Args:
        conn (_type_): _description_
        queries (_type_): _description_
        peer_table_dict (_type_): _description_
    """
    queries.update_peer_table_status_to_NPP_type_PR(
        conn,
        # peer_type=peer_table_dict["peer_type"],
        local_update_DTS=peer_table_dict["local_update_DTS"],
        version=peer_table_dict["version"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_status_to_PMP_type_PR(conn, queries, peer_table_dict):
    """
    Summary:

    _summary_

    Updates IPNS_name, id, signature, signature_valid, origin_update_DTS, local_update_DTS,
            execution_platform, python_version, IPFS_agent, processing_status, and agent
        based upon peer_ID

    Args:
        conn (_type_): _description_
        queries (_type_): _description_
        peer_table_dict (_type_): _description_
    """
    queries.update_peer_table_status_to_PMP_type_PR(
        conn,
        # peer_type=peer_table_dict["peer_type"],
        local_update_DTS=peer_table_dict["local_update_DTS"],
        version=peer_table_dict["version"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_status_to_NPC(conn, queries, peer_table_dict):
    """
    Summary:

    _summary_

    Updates IPNS_name, id, signature, signature_valid, origin_update_DTS, local_update_DTS,
            execution_platform, python_version, IPFS_agent, processing_status, and agent
        based upon peer_ID

    Args:
        conn (_type_): _description_
        queries (_type_): _description_
        peer_table_dict (_type_): _description_
    """

    queries.update_peer_table_status_to_NPC(
        conn,
        IPNS_name=peer_table_dict["IPNS_name"],
        id=peer_table_dict["id"],
        signature=peer_table_dict["signature"],
        signature_valid=peer_table_dict["signature_valid"],
        origin_update_DTS=peer_table_dict["origin_update_DTS"],
        local_update_DTS=peer_table_dict["local_update_DTS"],
        execution_platform=peer_table_dict["execution_platform"],
        python_version=peer_table_dict["python_version"],
        IPFS_agent=peer_table_dict["IPFS_agent"],
        agent=peer_table_dict["agent"],
        version=peer_table_dict["version"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_status_to_NPC_no_update(conn, queries, peer_table_dict):
    """
    Summary:

    _summary_

    Updates IPNS_name, id, signature, signature_valid, origin_update_DTS, local_update_DTS,
            execution_platform, python_version, IPFS_agent, processing_status, and agent
        based upon peer_ID

    Args:
        conn (_type_): _description_
        queries (_type_): _description_
        peer_table_dict (_type_): _description_
    """

    queries.update_peer_table_status_to_NPC_no_update(
        conn,
        local_update_DTS=peer_table_dict["local_update_DTS"],
        version=peer_table_dict["version"],
        peer_ID=peer_table_dict["peer_ID"],
    )
    return


def update_peer_table_metrics(conn, queries, peer_table_dict):
    queries.update_peer_table_metrics(
        conn,
        origin_update_DTS=peer_table_dict["origin_update_DTS"],
        execution_platform=peer_table_dict["execution_platform"],
        python_version=peer_table_dict["python_version"],
        IPFS_agent=peer_table_dict["IPFS_agent"],
        agent=peer_table_dict["agent"],
    )
    return


def refresh_peer_row_from_template():
    peer_row_dict = {}
    peer_row_dict["peer_ID"] = "null"
    peer_row_dict["IPNS_name"] = "null"
    peer_row_dict["id"] = "null"
    peer_row_dict["signature"] = "null"
    peer_row_dict["signature_valid"] = 0
    peer_row_dict["peer_type"] = "null"
    peer_row_dict["origin_update_DTS"] = "null"
    peer_row_dict["local_update_DTS"] = "null"
    peer_row_dict["execution_platform"] = "null"
    peer_row_dict["python_version"] = "null"
    peer_row_dict["IPFS_agent"] = "null"
    peer_row_dict["processing_status"] = "null"
    peer_row_dict["agent"] = "null"
    peer_row_dict["version"] = "0"

    return peer_row_dict


def export_local_peer_row(
    config_dict,
):  # NOTE: need a header table version by header cid
    conn, queries = set_up_sql_operations(config_dict)
    peer_table_dict = {}
    peer_table_entry = select_peer_table_local_peer_entry(
        conn, queries, peer_table_dict
    )

    peer_row_dict = {}
    peer_row_dict["peer_ID"] = peer_table_entry["peer_ID"]
    peer_row_dict["IPNS_name"] = peer_table_entry["IPNS_name"]
    peer_row_dict["id"] = peer_table_entry["id"]
    peer_row_dict["signature"] = peer_table_entry["signature"]
    peer_row_dict["signature_valid"] = peer_table_entry["signature_valid"]
    peer_row_dict["peer_type"] = peer_table_entry["peer_type"]
    peer_row_dict["origin_update_DTS"] = peer_table_entry["origin_update_DTS"]
    peer_row_dict["local_update_DTS"] = peer_table_entry["local_update_DTS"]
    peer_row_dict["execution_platform"] = peer_table_entry["execution_platform"]
    peer_row_dict["python_version"] = peer_table_entry["python_version"]
    peer_row_dict["IPFS_agent"] = peer_table_entry["IPFS_agent"]
    peer_row_dict["processing_status"] = peer_table_entry["processing_status"]
    peer_row_dict["agent"] = peer_table_entry["agent"]
    peer_row_dict["version"] = peer_table_entry["version"]

    conn.close()

    return peer_row_dict


def export_peer_row_for_peerID(
    peerID, config_dict
):  # NOTE: need a header table version by header cid
    conn, queries = set_up_sql_operations(config_dict)
    peer_table_dict = {}
    peer_table_dict["peer_ID"] = peerID
    peer_table_entry = select_peer_table_entry_by_key(conn, queries, peer_table_dict)

    peer_row_dict = {}
    peer_row_dict["peer_ID"] = peer_table_entry["peer_ID"]
    peer_row_dict["IPNS_name"] = peer_table_entry["IPNS_name"]
    peer_row_dict["id"] = peer_table_entry["id"]
    peer_row_dict["signature"] = peer_table_entry["signature"]
    peer_row_dict["signature_valid"] = peer_table_entry["signature_valid"]
    peer_row_dict["peer_type"] = peer_table_entry["peer_type"]
    peer_row_dict["origin_update_DTS"] = peer_table_entry["origin_update_DTS"]
    peer_row_dict["local_update_DTS"] = peer_table_entry["local_update_DTS"]
    peer_row_dict["execution_platform"] = peer_table_entry["execution_platform"]
    peer_row_dict["python_version"] = peer_table_entry["python_version"]
    peer_row_dict["IPFS_agent"] = peer_table_entry["IPFS_agent"]
    peer_row_dict["processing_status"] = peer_table_entry["processing_status"]
    peer_row_dict["agent"] = peer_table_entry["agent"]
    peer_row_dict["version"] = peer_table_entry["version"]

    conn.close()

    return peer_row_dict


def insert_network_row(conn, queries, network_table_dict):
    queries.insert_network_row(
        conn,
        network_name=network_table_dict["network_name"],
    )
    return


def select_network_name(conn, queries, network_table_dict):
    network_table_dict = queries.select_network_name(
        conn,
    )
    return network_table_dict


def refresh_network_table_from_template():
    network_table_dict = {}
    network_table_dict["version"] = "0"
    network_table_dict["network_name"] = "null"
    return network_table_dict


def insert_want_list_row(conn, queries, want_list_table_dict):
    # sql_str = get_sql_str()
    # queries = aiosql.from_str(sql_str, "sqlite3")

    queries.insert_want_list_row(
        conn,
        peer_ID=want_list_table_dict["peer_ID"],
        object_CID=want_list_table_dict["object_CID"],
        insert_DTS=want_list_table_dict["insert_DTS"],
        last_update_DTS=want_list_table_dict["last_update_DTS"],
        insert_update_delta=want_list_table_dict["insert_update_delta"],
        source_peer_type=want_list_table_dict["source_peer_type"],
    )
    return


def update_last_update_DTS(conn, queries, want_list_table_dict):
    # sql_str = get_sql_str()
    # queries = aiosql.from_str(sql_str, "sqlite3")

    queries.update_last_update_DTS(
        conn,
        last_update_DTS=want_list_table_dict["last_update_DTS"],
        insert_update_delta=want_list_table_dict["insert_update_delta"],
        peer_ID=want_list_table_dict["peer_ID"],
        object_CID=want_list_table_dict["object_CID"],
    )
    return


def select_want_list_entry_by_key(conn, queries, want_list_table_dict):
    # sql_str = get_sql_str()
    # queries = aiosql.from_str(sql_str, "sqlite3")

    want_list_entry = queries.select_want_list_entry_by_key(
        conn,
        peer_ID=want_list_table_dict["peer_ID"],
        object_CID=want_list_table_dict["object_CID"],
    )
    return want_list_entry


def refresh_want_list_table_dict():
    want_list_table_dict = {}
    want_list_table_dict["peer_ID"] = "null"
    want_list_table_dict["object_CID"] = "null"
    want_list_table_dict["insert_DTS"] = "null"
    want_list_table_dict["last_update_DTS"] = "null"
    want_list_table_dict["insert_update_delta"] = 0
    want_list_table_dict["source_peer_type"] = "null"
    return want_list_table_dict


def get_header_table_dict():
    header_table_dict = {}
    header_table_dict["version"] = "0"
    header_table_dict["object_CID"] = "null"
    header_table_dict["object_type"] = "null"
    header_table_dict["insert_DTS"] = "null"
    header_table_dict["prior_header_CID"] = "null"
    header_table_dict["header_CID"] = "null"
    header_table_dict["peer_ID"] = "null"
    header_table_dict["processing_status"] = "null"
    return header_table_dict


def refresh_header_dict_from_template():
    header_dict = {}
    header_dict["version"] = "0"
    header_dict["object_CID"] = "null"
    header_dict["object_type"] = "null"
    header_dict["insert_DTS"] = "null"
    header_dict["prior_header_CID"] = "null"
    header_dict["header_CID"] = "null"
    header_dict["peer_ID"] = "null"
    header_dict["processing_status"] = "null"

    return header_dict


def insert_header_row(conn, queries, header_dict, header_CID):
    queries.insert_header_row(
        conn,
        version=header_dict["version"],
        object_CID=header_dict["object_CID"],
        object_type=header_dict["object_type"],
        insert_DTS=header_dict["insert_DTS"],
        prior_header_CID=header_dict["prior_header_CID"],
        header_CID=header_CID,
        peer_ID=header_dict["peer_ID"],
        processing_status=header_dict["processing_status"],
    )


def insert_log_row(conn, queries, log_dict):
    queries.insert_log_row(
        conn,
        DTS=log_dict["DTS"],
        process=log_dict["process"],
        pid=log_dict["pid"],
        peer_type=log_dict["peer_type"],
        msg=log_dict["msg"],
    )
    return


def refresh_log_dict():
    log_dict = {}
    log_dict["DTS"] = "null"
    log_dict["process"] = "null"
    log_dict["pid"] = "null"
    log_dict["peer_type"] = "null"
    log_dict["msg"] = "null"
    return log_dict


def refresh_clean_up_dict():
    clean_up_dict = {}
    clean_up_dict["DTS"] = "null"
    clean_up_dict["want_item_file"] = "null"
    clean_up_dict["beacon_CID"] = "null"
    return clean_up_dict


def insert_clean_up_row(conn, queries, clean_up_dict):
    queries.insert_clean_up_row(
        conn,
        DTS=clean_up_dict["DTS"],
        want_item_file=clean_up_dict["want_item_file"],
        beacon_CID=clean_up_dict["beacon_CID"],
    )

    return


def select_clean_up_rows_by_date(conn, queries, clean_up_dict):
    with queries.select_clean_up_rows_by_date_cursor(
        conn,
        DTS=clean_up_dict["DTS"],
    ) as cursor:
        key_dict = {}
        i = 0
        cursor_tuples = cursor.fetchall()
        column_names = cursor.description
        for inner_tuple in column_names:
            key_dict[inner_tuple[0]] = i
            i += 1

    return cursor_tuples, key_dict


def delete_clean_up_row_by_date(conn, queries, clean_up_dict):
    queries.delete_clean_up_row_by_date(
        conn,
        DTS=clean_up_dict["DTS"],
    )

    return


def delete_log_rows_by_date(conn, queries, clean_up_dict):
    queries.delete_log_rows_by_date(
        conn,
        DTS=clean_up_dict["DTS"],
    )

    return


def delete_want_list_table_rows_by_date(conn, queries, clean_up_dict):
    queries.delete_want_list_table_rows_by_date(
        conn,
        DTS1=clean_up_dict["DTS"],
        DTS2=clean_up_dict["DTS"],
    )

    return


if __name__ == "__main__":
    reset_peer_table_status()
