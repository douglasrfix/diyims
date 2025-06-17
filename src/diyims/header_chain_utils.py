from datetime import datetime
from time import sleep
import psutil
from queue import Empty
from multiprocessing.managers import BaseManager
from sqlite3 import IntegrityError
from diyims.config_utils import get_beacon_config_dict
from diyims.database_utils import (
    insert_peer_row,
    set_up_sql_operations,
    insert_header_row,
    refresh_log_dict,
    insert_log_row,
    refresh_peer_row_from_template,
    # select_peer_table_entry_by_key,
    update_peer_table_status_to_PMP,
    update_peer_table_status_to_PMP_type_PR,
    select_shutdown_entry,
    add_header_chain_status_entry,
)
from diyims.requests_utils import execute_request
from diyims.logger_utils import get_logger
from diyims.ipfs_utils import get_url_dict, unpack_peer_row_from_cid
from diyims.general_utils import get_DTS


def monitor_peer_publishing():
    """
    docstring
    """
    test = 0
    p = psutil.Process()
    pid = p.pid
    ipfs_config_dict = get_beacon_config_dict()
    logger = get_logger(
        ipfs_config_dict["log_file"],
        "none",
    )
    url_dict = get_url_dict()

    logger.info("Peer Monitor startup.")
    q_server_port = int(ipfs_config_dict["q_server_port"])
    queue_server = BaseManager(address=("127.0.0.1", q_server_port), authkey=b"abc")
    queue_server.register("get_peer_maint_queue")
    queue_server.register("get_peer_monitor_queue")
    queue_server.connect()
    out_bound = queue_server.get_peer_maint_queue()
    in_bound = queue_server.get_peer_monitor_queue()

    while True:
        conn, queries = set_up_sql_operations(ipfs_config_dict)
        Rconn, Rqueries = set_up_sql_operations(ipfs_config_dict)
        peer_table_rows = Rqueries.select_peer_table_signature_valid(
            Rconn
        )  # TODO: change to function

        for row in peer_table_rows:  # peer level
            shutdown_row_dict = select_shutdown_entry(
                Rconn,
                Rqueries,
            )
            if shutdown_row_dict["enabled"]:
                break
            if (
                row["peer_type"] != "LP" and row["processing_status"] == "NPC"
            ):  # this single threads updates to a  remote peer
                ipns_path = "/ipns/" + row["IPNS_name"]

                start_DTS = get_DTS()
                param = {"arg": ipns_path}
                response, status_code, response_dict = execute_request(
                    url_key="resolve",
                    # logger=logger,
                    # url_dict=url_dict,
                    # config_dict=ipfs_config_dict,
                    param=param,  # may need timeout special
                )

                if status_code == 200:
                    stop_DTS = get_DTS()
                    start = datetime.fromisoformat(start_DTS)
                    stop = datetime.fromisoformat(stop_DTS)
                    duration = stop - start

                    peer_ID = row["peer_ID"]
                    msg = f"In {duration} resolve for {peer_ID}."
                    log_dict = refresh_log_dict()
                    log_dict["DTS"] = get_DTS()
                    log_dict["process"] = "monitor peer publishing"
                    log_dict["pid"] = pid
                    log_dict["peer_type"] = "CM"
                    log_dict["msg"] = msg
                    insert_log_row(conn, queries, log_dict)
                    conn.commit()

                    ipfs_header_CID = response_dict["Path"][6:]  # header cid in publish
                    db_header_row = Rqueries.select_last_header(  # TODO: change to function and name to newest header
                        Rconn, peer_ID=peer_ID
                    )  # last published cid that was processed

                    if (
                        db_header_row is None
                    ):  # we have not seen this peer before this costs one db read in exchange for one extra cat with an insert exception
                        header_chain_maint(
                            conn,
                            queries,
                            Rconn,
                            Rqueries,
                            ipfs_header_CID,
                            logger,
                            url_dict,
                            ipfs_config_dict,
                            pid,
                            out_bound,
                            peer_ID,
                        )  # add one or more headers

                    else:
                        most_recent_db_header = db_header_row["header_CID"]
                        # if we have a null cid a head of chain we should only process current entries
                        # if we hav e a gap whe should process the current entry and follow the chain tito see if we can fill the gap
                        # if we find the null entry we should delete any gap entry
                        if (
                            most_recent_db_header == ipfs_header_CID
                        ):  # nothing new #TODO: we should check for an existing gap and retry if there is one
                            pass
                            # print(f"no new entries for {peer_ID}")
                        else:
                            header_chain_maint(
                                conn,
                                queries,
                                Rconn,
                                Rqueries,
                                ipfs_header_CID,
                                logger,
                                url_dict,
                                ipfs_config_dict,
                                pid,
                                out_bound,
                                peer_ID,
                            )  # add one or more headers
                            # this assumes an in order arrival sequence dht delivers a best value as the most current

        try:
            if not test:
                in_bound.get(
                    timeout=600  # config value
                )  # comes from peer capture process
                shutdown_row_dict = select_shutdown_entry(
                    Rconn,
                    Rqueries,
                )
                if shutdown_row_dict["enabled"]:
                    break
            else:
                shutdown_row_dict = select_shutdown_entry(
                    Rconn,
                    Rqueries,
                )
                if shutdown_row_dict["enabled"]:
                    break
                sleep(60)  # config value

        except Empty:
            shutdown_row_dict = select_shutdown_entry(
                Rconn,
                Rqueries,
            )
            if shutdown_row_dict["enabled"]:
                break
        except AttributeError:
            shutdown_row_dict = select_shutdown_entry(
                Rconn,
                Rqueries,
            )
            if shutdown_row_dict["enabled"]:
                break
            sleep(60)  # config_value wait on queue ?

    conn.close()
    Rconn.close()
    logger.info("Peer Monitor shutdown.")
    return


def header_chain_maint(
    conn,
    queries,
    Rconn,
    Rqueries,
    ipfs_header_CID,
    logger,
    url_dict,
    config_dict,
    pid,
    out_bound,
    peer_ID,
):
    """
    docstring
    """

    # ipfs_config_dict = get_ipfs_config_dict()
    while True:
        start_DTS = get_DTS()
        # ipfs_header_CID =  "QmbY1Utuz753VwtQGyBvirB5Hgn8wouaRZ1xzBa99KaMRB"
        ipfs_path = "/ipfs/" + ipfs_header_CID

        param = {"arg": ipfs_path}
        response, status_code, response_dict = execute_request(
            url_key="cat",
            # logger=logger,
            # url_dict=url_dict,   # TODO: add to kwargs as override
            # config_dict=config_dict,
            param=param,
            # timeout=(3.05, 120),
            # connect_retries=0,
        )

        if status_code != 200:  # This could be caused by a time out or by a missing CID
            header_chain_status_dict = {}
            header_chain_status_dict["insert_DTS"] = get_DTS()
            header_chain_status_dict["peer_ID"] = peer_ID
            header_chain_status_dict["missing_header_CID"] = ipfs_header_CID
            header_chain_status_dict["message"] = "missing header"

            try:
                add_header_chain_status_entry(
                    conn,
                    queries,
                    header_chain_status_dict,
                )
                conn.commit()
            except IntegrityError:
                conn.commit()
                pass

            break  # log chain broken

        stop_DTS = get_DTS()
        start = datetime.fromisoformat(start_DTS)
        stop = datetime.fromisoformat(stop_DTS)
        duration = stop - start
        msg = f"In {duration} CAT {response_dict}."
        log_dict = refresh_log_dict()
        log_dict["DTS"] = get_DTS()
        log_dict["process"] = "header chain maint"
        log_dict["pid"] = pid
        log_dict["peer_type"] = "CM"
        log_dict["msg"] = msg
        insert_log_row(conn, queries, log_dict)
        conn.commit()

        object_type = response_dict["object_type"]
        object_CID = response_dict["object_CID"]

        if object_type == "local_peer_entry" or object_type == "provider_peer_entry":
            remote_peer_row_dict = unpack_peer_row_from_cid(object_CID, config_dict)

            proto_remote_peer_row_dict = refresh_peer_row_from_template()
            proto_remote_peer_row_dict["peer_ID"] = remote_peer_row_dict["peer_ID"]
            proto_remote_peer_row_dict["peer_type"] = "RP"
            proto_remote_peer_row_dict["version"] = object_CID
            proto_remote_peer_row_dict["local_update_DTS"] = get_DTS()
            proto_remote_peer_row_dict["processing_status"] = "PMP"

            try:
                insert_peer_row(conn, queries, proto_remote_peer_row_dict)
                conn.commit()
                out_bound.put_nowait("wake up")

                msg = f"Peer {remote_peer_row_dict['peer_ID']} added."
                log_dict = refresh_log_dict()
                log_dict["DTS"] = get_DTS()
                log_dict["process"] = "header chain maint"
                log_dict["pid"] = pid
                log_dict["peer_type"] = "CM"
                log_dict["msg"] = msg
                insert_log_row(conn, queries, log_dict)
                conn.commit()

            except IntegrityError:
                if object_type == "local_peer_entry":
                    # this will trigger peer maint by npp without change anything but the version, etc.
                    update_peer_table_status_to_PMP(
                        conn, queries, proto_remote_peer_row_dict
                    )
                    conn.commit()
                    out_bound.put_nowait("wake up")

                    msg = f"Peer {remote_peer_row_dict['peer_ID']} updated."
                    log_dict = refresh_log_dict()
                    log_dict["DTS"] = get_DTS()
                    log_dict["process"] = "header chain maint"
                    log_dict["pid"] = pid
                    log_dict["peer_type"] = "CM"
                    log_dict["msg"] = msg
                    insert_log_row(conn, queries, log_dict)
                    conn.commit()
                else:
                    # peer_row_dict = select_peer_table_entry_by_key(conn, queries, remote_peer_row_dict)

                    # from PP to PR to indicate an overlay if not already NPP or NPC  triggers a provider source change
                    proto_remote_peer_row_dict["peer_type"] = "PR"
                    update_peer_table_status_to_PMP_type_PR(
                        conn, queries, proto_remote_peer_row_dict
                    )
                    conn.commit()
                    out_bound.put_nowait("wake up")

                    msg = f"Peer {remote_peer_row_dict['peer_ID']} updated."
                    log_dict = refresh_log_dict()
                    log_dict["DTS"] = get_DTS()
                    log_dict["process"] = "header chain maint"
                    log_dict["pid"] = pid
                    log_dict["peer_type"] = "CM"
                    log_dict["msg"] = msg
                    insert_log_row(conn, queries, log_dict)
                    conn.commit()

        try:  # this method adds one extra cat to the process
            response_dict["processing_status"] = get_DTS()
            insert_header_row(conn, queries, response_dict, ipfs_header_CID)
            conn.commit()

        except IntegrityError:
            # pass will correct missing db components ########this leg shouldn't happen
            conn.rollback()
            break

        # this method eliminates the cat  abd insert exception and uses a db read instead

        ipfs_header_CID = response_dict["prior_header_CID"]

        if ipfs_header_CID == "null":
            header_chain_status_dict = {}
            header_chain_status_dict["insert_DTS"] = get_DTS()
            header_chain_status_dict["peer_ID"] = peer_ID
            header_chain_status_dict["missing_header_CID"] = ipfs_header_CID
            header_chain_status_dict["message"] = "Root header found"

            add_header_chain_status_entry(
                conn,
                queries,
                header_chain_status_dict,
            )
            conn.commit()
            break  # log chain complete

        db_header_row = Rqueries.select_header_CID(  # TODO: change to db function
            Rconn, header_CID=ipfs_header_CID
        )  # test for next entry

        if (
            db_header_row is not None
        ):  # If not missing, this will add to the chain until the prior is null
            break

    # a missing cid ot time out currently goes here
    # a nieve assumption would be to treat it as a missing cid

    # log gap
    return


if __name__ == "__main__":
    monitor_peer_publishing()
