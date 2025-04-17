from time import sleep
import json
import requests
from requests.exceptions import ConnectionError, HTTPError, ConnectTimeout, ReadTimeout


def execute_request(url_key, **kwargs):
    try:
        url_dict = kwargs["url_dict"]
    except KeyError:
        url_dict = ""
    try:
        config_dict = kwargs["config_dict"]
    except KeyError:
        config_dict = ""
    try:
        param = kwargs["param"]
    except KeyError:
        param = ""
    try:
        file = kwargs["file"]
    except KeyError:
        file = ""
    try:
        logger = kwargs["logger"]
    except KeyError:
        logger = ""
    try:
        timeout_tuple = kwargs["timeout"]
    except KeyError:
        timeout_tuple = None

    if config_dict:
        connect_retries = int(config_dict["connect_retries"])
        connect_retry_delay = int(config_dict["connect_retry_delay"])
    else:
        connect_retries = 30
        connect_retry_delay = 10

    retry = 0
    response_ok = False
    while retry < connect_retries and not response_ok:
        try:
            if file:
                with requests.post(
                    url_dict[url_key],
                    params=param,
                    files=file,
                    stream=False,
                    timeout=timeout_tuple,
                ) as r:
                    r.raise_for_status()
                    response_ok = True
            else:
                with requests.post(
                    url=url_dict[url_key],
                    params=param,
                    stream=False,
                    timeout=timeout_tuple,
                ) as r:
                    r.raise_for_status()
                    response_ok = True

        except ConnectTimeout:
            if logger:
                logger.exception()
            status_code = 601
            sleep(connect_retry_delay)
            retry += 1
        except ReadTimeout:
            status_code = 602
            break
        except HTTPError:
            if logger:
                logger.exception(param)
            status_code = 500
            break
        except ConnectionError:
            if logger:
                logger.exception(status_code=603)
            sleep(connect_retry_delay)
            retry += 1
    # print(timeout_tuple)
    if not response_ok:
        r = 600
    else:
        status_code = r.status_code
        response_dict = json.loads(r.text)

    return r, status_code, response_dict
