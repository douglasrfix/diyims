def test():
    """ """
    from diyims.database_utils import (
        set_up_sql_operations,
        select_peer_table_entry_by_key,
    )
    from diyims.config_utils import get_metrics_config_dict

    config_dict = get_metrics_config_dict()
    Rconn, Rqueries = set_up_sql_operations(config_dict)
    peer_row_dict = {}
    peer_row_dict["peer_ID"] = "12D3KooWEVZcNKwWkxC9edn13qxMsG3bYLmmWkmz3XCrqdCV7Uj1"
    peer_table_entry = select_peer_table_entry_by_key(Rconn, Rqueries, peer_row_dict)
    for entry in peer_table_entry:
        print(entry)
    return


if __name__ == "__main__":
    test()
