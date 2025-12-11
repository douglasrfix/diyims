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
import os
from requests.exceptions import HTTPError, ConnectTimeout, ReadTimeout, RequestException
from diyims.config_utils import get_request_config_dict, get_url_dict
from diyims.logger_utils import add_log


def execute_request(url_key: str, **kwargs):
    """
    execute_request _summary_

    _extended_summary_

    Arguments:
        url_key {str} -- _description_

    Returns:
        _type_ -- _description_
    """
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
    connect_retry = -1
    request_retry = -1
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

    try:
        url_dict = value_dict["url_dict"]
    except KeyError:
        url_dict = get_url_dict()

    queues_enabled = bool(int(value_dict["queues_enabled"]))
    try:
        queues_enabled = bool(int(os.environ["QUEUES_ENABLED"]))
    except KeyError:
        pass
    try:
        component_test = bool(int(os.environ["COMPONENT_TEST"]))
    except KeyError:
        component_test = False
    logging_enabled = bool(int(value_dict["logging_enabled"]))
    debug_enabled = bool(int(value_dict["debug_enabled"]))
    dummy = component_test
    dummy = queues_enabled
    dummy = debug_enabled
    dummy = dummy
    while (
        connect_retry < int(value_dict["connect_retries"])
        and request_retry < int(value_dict["request_retries"])
        and not response_ok
    ):
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
        except ConnectTimeout as e:
            status_code = 601
            if logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type=f"comm_status {status_code}",
                    msg=f"{url_key} after {connect_retry} failing with {e}",
                )

            sleep(int(value_dict["connect_retry_delay"]))
            r = None
            connect_retry += 1
            if connect_retry > 0:
                sleep(int(value_dict["connect_retry_delay"]))
        except ReadTimeout as e:
            status_code = 602
            if logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type=f"http_status {status_code}",
                    msg=f"{url_key} after {connect_retry} failing with {e}",
                )

            r = None
            connect_retry += 1
            if connect_retry > 0:
                sleep(int(value_dict["connect_retry_delay"]))
        except HTTPError as e:
            status_code = r.status_code
            if ignore_500:
                if logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type=f"http_status {status_code}",
                        msg=f"{url_key} ignored after failing with {e}",
                    )
                break
            else:
                if logging_enabled:
                    add_log(
                        process=call_stack,
                        peer_type=f"http_status {status_code}",
                        msg=f"{url_key} after {connect_retry} failing with {e}",
                    )
                connect_retry += 1
                if connect_retry > 0:
                    sleep(int(value_dict["connect_retry_delay"]))
        except RequestException as e:
            status_code = 700
            if logging_enabled:
                add_log(
                    process=call_stack,
                    peer_type=f"comm_status {status_code}",
                    msg=f"{url_key} after {request_retry} failing with {e}",
                )
            request_retry += 1
            r = None
            if connect_retry > 0:
                sleep(int(value_dict["request_retry_delay"]))
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
