from datetime import datetime
from time import sleep
import psutil
from sqlite3 import IntegrityError
from diyims.config_utils import get_ipfs_config_dict
from diyims.database_utils import (
    set_up_sql_operations,
    insert_header_row,
    refresh_log_dict,
    insert_log_row,
)
from diyims.requests_utils import execute_request
from diyims.logger_utils import get_logger
from diyims.ipfs_utils import get_url_dict
from diyims.general_utils import get_DTS


def monitor_peer_publishing():
    """
    docstring
    """

    p = psutil.Process()
    pid = p.pid

    ipfs_config_dict = get_ipfs_config_dict()
    logger = get_logger(
        ipfs_config_dict["log_file"],
        "none",
    )
    url_dict = get_url_dict()

    while True:
        conn, queries = set_up_sql_operations(ipfs_config_dict)
        Rconn, Rqueries = set_up_sql_operations(ipfs_config_dict)
        peer_table_rows = Rqueries.select_peer_table_signature_valid(Rconn)

        for row in peer_table_rows:  # peer level
            if row["peer_type"] != "LP":
                ipns_path = "/ipns/" + row["IPNS_name"]

                start_DTS = get_DTS()
                param = {"arg": ipns_path}
                response, status_code, response_dict = execute_request(
                    url_key="resolve",
                    logger=logger,
                    url_dict=url_dict,
                    config_dict=ipfs_config_dict,
                    param=param,
                )
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

                ipfs_header_CID = response_dict["Path"][6:]
                db_header_row = Rqueries.select_last_header(Rconn, peer_ID=peer_ID)

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
                    )  # add one or more headers

                else:
                    most_recent_db_header = db_header_row["header_CID"]
                    if most_recent_db_header == ipfs_header_CID:  # nothing new
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
                        )  # add one or more headers
                        # this assumes an in order arrival sequence dht delivers a best value as the most current

        conn.close()
        sleep(600)

    return


def header_chain_maint(
    conn,
    queries,
    Rconn,
    Rqueries,
    ipfs_header_CID,
    logger,
    url_dict,
    ipfs_config_dict,
    pid,
):
    """
    docstring
    """

    while True:
        start_DTS = get_DTS()
        ipfs_path = "/ipfs/" + ipfs_header_CID
        param = {"arg": ipfs_path}
        response, status_code, response_dict = execute_request(
            url_key="cat",
            logger=logger,
            url_dict=url_dict,
            config_dict=ipfs_config_dict,
            param=param,
            timeout=(3.05, 120),
        )

        if status_code != 200:
            break

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

        try:  # this method adds one extra cat to the process
            insert_header_row(conn, queries, response_dict, ipfs_header_CID)
            conn.commit()

        except IntegrityError:
            # pass will correct missing db components
            break

        # this method eliminates the cat  abd insert exception and uses a db read instead
        ipfs_header_CID = response_dict["prior_header_CID"]

        if ipfs_header_CID == "null":  # no more headers
            break

        db_header_row = Rqueries.select_header_CID(
            Rconn, header_CID=ipfs_header_CID
        )  # test for missing entry

        if db_header_row is not None:  # If not missing
            break

    return


if __name__ == "__main__":
    monitor_peer_publishing()
