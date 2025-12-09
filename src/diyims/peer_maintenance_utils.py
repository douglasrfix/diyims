# import os
# from datetime import datetime, timedelta, timezone
# from time import sleep
# from sqlite3 import IntegrityError
from sqlmodel import create_engine, Session, select  # ,col

from sqlalchemy.exc import NoResultFound

# from multiprocessing import set_start_method, freeze_support
# from multiprocessing.managers import BaseManager
# from queue import Empty
# from diyims.requests_utils import execute_request
# from diyims.database_utils import (
#    insert_want_list_row,
#    select_want_list_entry_by_key,
#    update_last_update_DTS,
#    refresh_peer_row_from_template,
#    refresh_want_list_table_dict,
#    set_up_sql_operations,
#    update_peer_table_status_WLR,
#    update_peer_table_status_WLX,
#    update_peer_table_status_WLZ,
#    update_peer_table_status_to_NPP,
#    select_peer_table_entry_by_key,
# )
# from diyims.general_utils import get_DTS, shutdown_query
# from diyims.ipfs_utils import unpack_object_from_cid
# from diyims.logger_utils import add_log
# from diyims.config_utils import get_want_list_config_dict
from diyims.path_utils import get_path_dict
from diyims.sqlmodels import Peer_Table
# from diyims.class_imports import WantlistCaptureProcessMainArgs, SetControlsReturn


def peer_add_update(call_stack, execution_mode, peer_maint_dict):
    call_stack = call_stack + ":peer_add_update"
    path_dict = get_path_dict()
    connect_path = path_dict["db_file"]
    db_url = f"sqlite:///{connect_path}"
    engine = create_engine(db_url, echo=False, connect_args={"timeout": 120})
    peer_dict = {}

    if execution_mode == "read":
        peer_ID = peer_maint_dict["peer_ID"]
        statement = select(Peer_Table).where(Peer_Table.peer_ID == peer_ID)
        with Session(engine) as session:
            results = session.exec(statement)
            try:
                current_peer = results.one()
                peer_dict["processing_status"] = current_peer.processing_status
                status_code = 200
            except NoResultFound:
                status_code = 402

    return status_code, peer_dict
