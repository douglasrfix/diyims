from datetime import datetime

# from time import sleep

# import psutil
import os

# from queue import Empty
# from multiprocessing.managers import BaseManager
from multiprocessing import set_start_method, freeze_support
from sqlite3 import IntegrityError

# from diyims.config_utils import get_beacon_config_dict
from diyims.database_utils import (
    insert_peer_row,
    set_up_sql_operations,
    insert_header_row,
    # refresh_log_dict,
    # insert_log_row,
    refresh_peer_row_from_template,
    # select_peer_table_entry_by_key,
    update_peer_table_status_to_PMP,
    update_peer_table_status_to_PMP_type_PR,
    # select_shutdown_entry,
    add_header_chain_status_entry,
)
from diyims.requests_utils import execute_request
from diyims.logger_utils import add_log
from diyims.ipfs_utils import unpack_peer_row_from_cid
from diyims.general_utils import get_DTS
# from diyims.path_utils import get_path_dict

# from sqlalchemy.exc import NoResultFound
# from sqlmodel import create_engine, Session, select
# from diyims.sqlmodels import Peer_Table
# from rich import print


def header_chain_maintT(
    call_stack,
    ipfs_header_CID,
    config_dict,
    out_bound,
    peer_ID,
    logging_enabled,
):
    """
    docstring
    """
    call_stack = call_stack + ":header_chain_maint"
    status_code = 200
    # ipfs_config_dict = get_ipfs_config_dict()
    while True:
        start_DTS = get_DTS()
        # ipfs_header_CID =  "QmbY1Utuz753VwtQGyBvirB5Hgn8wouaRZ1xzBa99KaMRB"
        ipfs_path = "/ipfs/" + ipfs_header_CID

        param = {"arg": ipfs_path}
        response, status_code, response_dict = execute_request(
            url_key="cat",
            param=param,
            timeout=(3.05, 122),  # avoid timeouts
            call_stack=call_stack,
            http_500_ignore=False,
        )
        if status_code != 200:  # TODO: disable peer after ????
            header_chain_status_dict = {}
            header_chain_status_dict["insert_DTS"] = get_DTS()
            header_chain_status_dict["peer_ID"] = peer_ID
            header_chain_status_dict["missing_header_CID"] = ipfs_header_CID
            header_chain_status_dict["message"] = "missing header"
            conn, queries = set_up_sql_operations(config_dict)
            try:
                add_header_chain_status_entry(
                    conn,
                    queries,
                    header_chain_status_dict,
                )
                conn.commit()
                conn.close()
            except IntegrityError:
                conn.rollback()
                conn.close()
                pass

            break  # log chain broken

        stop_DTS = get_DTS()
        start = datetime.fromisoformat(start_DTS)
        stop = datetime.fromisoformat(stop_DTS)
        duration = stop - start
        msg = f"In {duration} CAT {response_dict}."
        if logging_enabled:
            add_log(
                process=call_stack,
                peer_type="status",
                msg=msg,
            )

        object_type = response_dict["object_type"]
        object_CID = response_dict["object_CID"]

        if object_type == "local_peer_entry" or object_type == "provider_peer_entry":
            status_code, remote_peer_row_dict = unpack_peer_row_from_cid(
                call_stack, object_CID, config_dict
            )
            # TODO: disable peer if != 200

            proto_remote_peer_row_dict = refresh_peer_row_from_template()
            proto_remote_peer_row_dict["peer_ID"] = remote_peer_row_dict["peer_ID"]
            proto_remote_peer_row_dict["peer_type"] = "RP"
            proto_remote_peer_row_dict["version"] = object_CID
            proto_remote_peer_row_dict["local_update_DTS"] = get_DTS()
            proto_remote_peer_row_dict["processing_status"] = "PMP"
            conn, queries = set_up_sql_operations(config_dict)
            try:
                insert_peer_row(conn, queries, proto_remote_peer_row_dict)
                conn.commit()
                conn.close()
                # out_bound.put_nowait("wake up")

                msg = f"Peer {remote_peer_row_dict['peer_ID']} added."
                if logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type="status",
                        msg=msg,
                    )

            except IntegrityError:
                conn.rollback()
                conn.close()
                if object_type == "local_peer_entry":
                    # this will trigger peer maint by npp without change anything but the version, etc.
                    conn, queries = set_up_sql_operations(config_dict)
                    update_peer_table_status_to_PMP(
                        conn, queries, proto_remote_peer_row_dict
                    )
                    conn.commit()
                    conn.close()
                    # out_bound.put_nowait("wake up")

                    msg = f"Peer {remote_peer_row_dict['peer_ID']} updated."
                    if logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=msg,
                        )
                else:
                    # peer_row_dict = select_peer_table_entry_by_key(conn, queries, remote_peer_row_dict)

                    # from PP to PR to indicate an overlay if not already NPP or NPC  triggers a provider source change
                    proto_remote_peer_row_dict["peer_type"] = "PR"
                    conn, queries = set_up_sql_operations(config_dict)
                    update_peer_table_status_to_PMP_type_PR(
                        conn, queries, proto_remote_peer_row_dict
                    )
                    conn.commit()
                    conn.close()
                    # out_bound.put_nowait("wake up")

                    msg = f"Peer {remote_peer_row_dict['peer_ID']} updated."
                    if logging_enabled:
                        add_log(
                            process=call_stack,
                            peer_type="status",
                            msg=msg,
                        )

        conn, queries = set_up_sql_operations(config_dict)
        try:  # this method adds one extra cat to the process
            response_dict["processing_status"] = get_DTS()
            insert_header_row(conn, queries, response_dict, ipfs_header_CID)
            conn.commit()
            conn.close()

        except IntegrityError:
            # pass will correct missing db components ########this leg shouldn't happen
            conn.rollback()
            conn.close()
            break

        # this method eliminates the cat  abd insert exception and uses a db read instead

        ipfs_header_CID = response_dict["prior_header_CID"]

        if ipfs_header_CID == "null":
            header_chain_status_dict = {}
            header_chain_status_dict["insert_DTS"] = get_DTS()
            header_chain_status_dict["peer_ID"] = peer_ID
            header_chain_status_dict["missing_header_CID"] = ipfs_header_CID
            header_chain_status_dict["message"] = "Root header found"
            conn, queries = set_up_sql_operations(config_dict)
            add_header_chain_status_entry(
                conn,
                queries,
                header_chain_status_dict,
            )
            conn.commit()
            conn.close()
            break  # log chain complete
        conn, queries = set_up_sql_operations(config_dict)
        db_header_row = queries.select_header_CID(  # TODO: change to db function
            conn, header_CID=ipfs_header_CID
        )  # test for next entry
        conn.close()
        if (
            db_header_row is not None
        ):  # If not missing, this will add to the chain until the prior is null
            break

    # a missing cid ot time out currently goes here
    # a nieve assumption would be to treat it as a missing cid

    # log gap
    return status_code


if __name__ == "__main__":
    import os
    from multiprocessing import set_start_method, freeze_support

    freeze_support()
    set_start_method("spawn")

    os.environ["DIYIMS_ROAMING"] = "RoamingDev"
    os.environ["COMPONENT_TEST"] = "1"
    os.environ["QUEUES_ENABLED"] = "0"
    # monitor_peer_publishing_main("__main__")
