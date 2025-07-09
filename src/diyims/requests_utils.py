from time import sleep
import json
import requests
from requests.exceptions import ConnectionError, HTTPError, ConnectTimeout, ReadTimeout
from diyims.config_utils import get_request_config_dict, get_url_dict


def execute_request(url_key, **kwargs):
    default_dict = get_request_config_dict()
    url_dict = get_url_dict()
    try:
        default_dict["timeout"] = kwargs["timeout"]
    except KeyError:
        default_dict["timeout"] = tuple(
            [float(default_dict["connect_timeout"]), int(default_dict["read_timeout"])]
        )

    default_dict["param"] = ""
    default_dict["file"] = ""

    value_dict = {**default_dict, **kwargs}

    retry = -1
    response_ok = False

    if value_dict["stream"] == "False":
        stream = False
    else:
        stream = True

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
        except ReadTimeout:
            status_code = 602
            retry += 1
        except HTTPError:
            status_code = r.status_code
            break
        except ConnectionError:
            status_code = 603
            retry += 1

    if not response_ok:
        response_dict = {}
    else:
        status_code = r.status_code
        try:
            response_dict = json.loads(r.text)
        except json.JSONDecodeError:
            response_dict = {}

    return r, status_code, response_dict
