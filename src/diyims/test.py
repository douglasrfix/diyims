# from datetime import datetime, timedelta, timezone

# from pathlib import Path
from rich import print
# import requests

from diyims.config_utils import get_want_list_config_dict

# from diyims.path_utils import get_path_dict
from diyims.database_utils import (
    set_up_sql_operations,
)


def package_peer_table():
    """
    docstring
    """
    config_dict = get_want_list_config_dict()
    conn, queries = set_up_sql_operations(config_dict)
    peer_table_rows = queries.select_peer_table_signature_valid(conn)
    peer_table_dict = {}
    for row in peer_table_rows:
        row_key_list = row.keys()

        peer_dict = {}
        for key in row_key_list:
            peer_dict[key] = row[key]

        # peer_table_dict["peer_ID"] = row["peer_ID"]
        peer_table_dict[row["peer_ID"]] = peer_dict
        print(peer_table_dict)
        print(peer_dict)
    return


def test():
    package_peer_table()
    return


if __name__ == "__main__":
    test()
