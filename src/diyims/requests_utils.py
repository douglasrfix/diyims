"""
Wraps "requests" so the various errors and defaults amy be handled.

Initial values are provided via the configuration file (default_dict) used by the application.
The configuration values are supplemented by internal default values which may include reformating.
The configuration values are then merged with an overrides provided by key word arguments.
This is followed by setting the values to be used by the application.
"""

from time import sleep
import json
import requests
from requests.exceptions import ConnectionError, HTTPError, ConnectTimeout, ReadTimeout
from diyims.config_utils import get_request_config_dict, get_url_dict


def execute_request(url_key: str, **kwargs):
    # get configuration values
    default_dict = get_request_config_dict()

    # supplement configuration values
    try:
        default_dict["timeout"] = kwargs["timeout"]
    except KeyError:
        default_dict["timeout"] = tuple(
            [float(default_dict["connect_timeout"]), int(default_dict["read_timeout"])]
        )

    default_dict["param"] = ""
    default_dict["file"] = ""
    default_dict["http_500_ignore"] = "True"

    # merge default values with key word values. This may or may not overwrite default values.
    value_dict = {**default_dict, **kwargs}

    # establish values for operational use
    retry = -1
    response_ok = False

    if value_dict["http_500_ignore"] == "True":
        ignore_500 = True
    else:
        ignore_500 = False

    if value_dict["stream"] == "False":
        stream = False
    else:
        stream = True

    try:
        call_stack = value_dict["call_stack"]
        call_stack = call_stack + ":execute_request"

    except KeyError:
        call_stack = "execute_request"

    url_dict = get_url_dict()

    while retry < int(value_dict["connect_retries"]) and not response_ok:
        if retry > 0:
            sleep(int(value_dict["connect_retry_delay"]))

        try:
            with requests.post(
                url=url_dict[url_key],
                params=value_dict["param"],
                files=value_dict["file"],
                stream=stream,
                timeout=value_dict["timeout"],
            ) as r:
                r.raise_for_status()
                status_code = r.status_code
                response_ok = True

        except ConnectTimeout:
            status_code = 601
            sleep(int(value_dict["connect_retry_delay"]))
            retry += 1
            r = None
        except ReadTimeout:
            status_code = 602
            retry += 1
            r = None
        except HTTPError:  # TODO: expand errors handled?
            if ignore_500:
                status_code = r.status_code
                add_log(
                    process=call_stack,
                    peer_type=f"http_status {status_code}",
                    msg=f"{url_key} failed and ignored",
                )
                break
            else:
                add_log(
                    process=call_stack,
                    peer_type=f"http_status {status_code}",
                    msg=f"{url_key} failed and retried",
                )
                sleep(int(value_dict["connect_retry_delay"]))
                retry += 1  # TODO: this needs its own retry logic
                status_code = 700

        except ConnectionError:
            status_code = 603
            retry += 1
            r = None

    if not response_ok:
        response_dict = {}
        r = None
    else:
        status_code = r.status_code
        try:
            response_dict = json.loads(r.text)
        except json.JSONDecodeError:
            response_dict = {}

    return r, status_code, response_dict


def add_log(process: str, peer_type: str, msg: str):
    import psutil
    from diyims.general_utils import get_DTS
    from diyims.sqlmodels import Log
    from diyims.path_utils import get_path_dict
    from sqlmodel import Session, create_engine

    p = psutil.Process()
    pid = p.pid

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"

    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    log_entry = Log(
        DTS=get_DTS(), process=process, pid=pid, peer_type=peer_type, msg=msg
    )

    with Session(engine) as session:
        session.add(log_entry)
        session.commit()
