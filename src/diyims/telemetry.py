import json
import os
from multiprocessing import set_start_method, freeze_support
from diyims.platform_utils import get_python_version, test_os_platform
from diyims.ipfs_utils import test_ipfs_version
from diyims.config_utils import get_metrics_config_dict
from diyims.general_utils import get_DTS, get_agent, set_self, set_controls
from diyims.path_utils import get_path_dict
from diyims.logger_utils import add_log
from diyims.ipfs_utils import get_url_dict
from diyims.header_utils import ipfs_header_add
from diyims.requests_utils import execute_request
from sqlmodel import create_engine, Session, select
from diyims.sqlmodels import Peer_Table, Peer_Telemetry
from fastapi.encoders import jsonable_encoder


def select_local_peer_and_update_metrics(call_stack):
    """ """
    # TODO: add message
    call_stack = call_stack + ":select_local_peer_and_update_metrics"
    config_dict = get_metrics_config_dict()
    url_dict = get_url_dict()
    path_dict = get_path_dict()
    SetControlsReturn = set_controls(call_stack, config_dict)
    SetSelfReturn = set_self()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
    status_code = 200
    statement = (
        select(Peer_Table)
        .where(Peer_Table.peer_ID == SetSelfReturn.self)
        .where(Peer_Table.peer_type == "LP")
    )
    with Session(engine) as session:
        results = session.exec(statement)
        peer = results.one()

    statement = select(Peer_Telemetry).where(Peer_Telemetry.peer_ID == peer.peer_ID)
    with Session(engine) as session:
        results = session.exec(statement)
        telemetry = results.one()

    IPFS_agent = test_ipfs_version()
    os_platform = test_os_platform()
    python_version = get_python_version()
    agent = get_agent()

    changed_metrics = False

    if telemetry.execution_platform != os_platform:
        telemetry.execution_platform = os_platform
        changed_metrics = True

    if telemetry.python_version != python_version:
        telemetry.python_version = python_version
        changed_metrics = True

    if telemetry.IPFS_agent != IPFS_agent:
        telemetry.IPFS_agent = IPFS_agent
        changed_metrics = True

    if telemetry.DIYIMS_agent != agent:
        telemetry.DIYIMS_agent = agent
        changed_metrics = True

    if changed_metrics:
        DTS = get_DTS()

        telemetry.update_DTS = DTS
        # telemetry_dict = dict(telemetry)
        telemetry_dict = jsonable_encoder(telemetry)

        with Session(engine) as session:
            session.add(telemetry)
            session.commit()

        proto_file = path_dict["peer_file"]
        param = {
            "cid-version": 1,
            "only-hash": "false",
            "pin": "true",
            "pin-name": "update_telemetry",
        }
        with open(proto_file, "w", encoding="utf-8", newline="\n") as write_file:
            json.dump(telemetry_dict, write_file, indent=4)

        f = open(proto_file, "rb")
        add_file = {"file": f}
        response, status_code, response_dict = execute_request(
            url_key="add",
            url_dict=url_dict,
            config_dict=config_dict,
            file=add_file,
            param=param,
            call_stack=call_stack,
            http_500_ignore=False,
        )
        f.close()

        if status_code != 200:
            add_log(
                process=call_stack,
                peer_type="Error",
                msg="Update Telemetry Object_CID add failed Panic.",
            )
            return status_code

        peer_ID = peer.peer_ID
        object_CID = response_dict["Hash"]  # new peer row cid
        object_type = "telemetry_entry"
        mode = object_type
        processing_status = DTS

        status_code, header_CID = ipfs_header_add(
            call_stack,
            DTS,
            object_CID,
            object_type,
            peer_ID,
            config_dict,
            mode,
            processing_status,
            SetControlsReturn.queues_enabled,
        )

        if status_code != 200:  # TODO: remove recent pin or defer pin to here
            add_log(
                process=call_stack,
                peer_type="Error",
                msg="Update Telemetry Panic.",
            )
            return status_code

    return status_code


if __name__ == "__main__":
    freeze_support()
    set_start_method("spawn")

    os.environ["DIYIMS_ROAMING"] = "Roaming"
    os.environ["COMPONENT_TEST"] = "1"
    os.environ["QUEUES_ENABLED"] = "0"
    select_local_peer_and_update_metrics("__main__")
