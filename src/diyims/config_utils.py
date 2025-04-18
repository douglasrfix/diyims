import configparser
import json
import requests
from time import sleep
from pathlib import Path
from diyims.ipfs_utils import get_url_dict
from diyims.error_classes import ApplicationNotInstalledError
from diyims.path_utils import (
    get_install_template_dict,
)
# NOTE: update defaults and provide a set to system defaults function


def config_install():
    get_scheduler_config_dict()
    get_beacon_config_dict()
    get_satisfy_config_dict()
    get_capture_peer_config_dict()
    get_want_list_config_dict()
    get_queue_config_dict()
    get_logger_config_dict()
    get_logger_server_config_dict()
    get_ipfs_config_dict()
    get_clean_up_config_dict()

    return


def get_beacon_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        beacon_config_dict = {}
        beacon_config_dict["long_period_seconds"] = parser["Beacon"][
            "long_period_seconds"
        ]
        beacon_config_dict["short_period_seconds"] = parser["Beacon"][
            "short_period_seconds"
        ]
        beacon_config_dict["number_of_periods"] = parser["Beacon"]["number_of_periods"]
        beacon_config_dict["sql_timeout"] = parser["Beacon"]["sql_timeout"]
        beacon_config_dict["wait_before_startup"] = parser["Beacon"][
            "wait_before_startup"
        ]
        beacon_config_dict["beacon_pin_enabled"] = parser["Beacon"][
            "beacon_pin_enabled"
        ]
        beacon_config_dict["max_intervals"] = parser["Beacon"]["max_intervals"]
        beacon_config_dict["shutdown_time"] = parser["Beacon"]["shutdown_time"]
        beacon_config_dict["q_server_port"] = parser["Beacon"]["q_server_port"]
        beacon_config_dict["log_file"] = parser["Beacon"]["log_file"]

        beacon_config_dict["connect_retries"] = parser["Beacon"]["connect_retries"]
        beacon_config_dict["connect_retry_delay"] = parser["Beacon"][
            "connect_retry_delay"
        ]
    except KeyError:
        parser["Beacon"] = {}
        parser["Beacon"]["long_period_seconds"] = "300"
        parser["Beacon"]["short_period_seconds"] = "300"
        parser["Beacon"]["number_of_periods"] = "5"
        parser["Beacon"]["sql_timeout"] = "60"
        parser["Beacon"]["wait_before_startup"] = "0"
        parser["Beacon"]["beacon_pin_enabled"] = "0"
        parser["Beacon"]["max_intervals"] = "1440"
        parser["Beacon"]["shutdown_time"] = "22:0:0"
        parser["Beacon"]["q_server_port"] = "50000"
        parser["Beacon"]["log_file"] = "beacon.log"
        parser["Beacon"]["connect_retries"] = "30"
        parser["Beacon"]["connect_retry_delay"] = "10"
        with open(config_file, "w") as configfile:
            parser.write(configfile)

        beacon_config_dict = {}
        beacon_config_dict["long_period_seconds"] = parser["Beacon"][
            "long_period_seconds"
        ]
        beacon_config_dict["short_period_seconds"] = parser["Beacon"][
            "short_period_seconds"
        ]
        beacon_config_dict["number_of_periods"] = parser["Beacon"]["number_of_periods"]
        beacon_config_dict["sql_timeout"] = parser["Beacon"]["sql_timeout"]
        beacon_config_dict["wait_before_startup"] = parser["Beacon"][
            "wait_before_startup"
        ]
        beacon_config_dict["beacon_pin_enabled"] = parser["Beacon"][
            "beacon_pin_enabled"
        ]
        beacon_config_dict["max_intervals"] = parser["Beacon"]["max_intervals"]
        beacon_config_dict["shutdown_time"] = parser["Beacon"]["shutdown_time"]
        beacon_config_dict["q_server_port"] = parser["Beacon"]["q_server_port"]
        beacon_config_dict["log_file"] = parser["Beacon"]["log_file"]

        beacon_config_dict["connect_retries"] = parser["Beacon"]["connect_retries"]
        beacon_config_dict["connect_retry_delay"] = parser["Beacon"][
            "connect_retry_delay"
        ]

    return beacon_config_dict


def get_satisfy_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        satisfy_config_dict = {}
        satisfy_config_dict["wait_before_startup"] = parser["Satisfy"][
            "wait_before_startup"
        ]
        satisfy_config_dict["q_server_port"] = parser["Satisfy"]["q_server_port"]
        satisfy_config_dict["log_file"] = parser["Satisfy"]["log_file"]
        satisfy_config_dict["connect_retries"] = parser["Satisfy"]["connect_retries"]
        satisfy_config_dict["connect_retry_delay"] = parser["Satisfy"][
            "connect_retry_delay"
        ]

    except KeyError:
        parser["Satisfy"] = {}
        parser["Satisfy"]["wait_before_startup"] = "10"
        parser["Satisfy"]["q_server_port"] = "50000"
        parser["Satisfy"]["log_file"] = "satisfy.log"
        parser["Satisfy"]["connect_retries"] = "30"
        parser["Satisfy"]["connect_retry_delay"] = "10"
        with open(config_file, "w") as configfile:
            parser.write(configfile)

        satisfy_config_dict = {}
        satisfy_config_dict["wait_before_startup"] = parser["Satisfy"][
            "wait_before_startup"
        ]
        satisfy_config_dict["q_server_port"] = parser["Satisfy"]["q_server_port"]
        satisfy_config_dict["log_file"] = parser["Satisfy"]["log_file"]
        satisfy_config_dict["connect_retries"] = parser["Satisfy"]["connect_retries"]
        satisfy_config_dict["connect_retry_delay"] = parser["Satisfy"][
            "connect_retry_delay"
        ]

    return satisfy_config_dict


def get_scheduler_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        scheduler_config_dict = {}
        scheduler_config_dict["beacon_enable"] = parser["Scheduler"]["beacon_enable"]
        scheduler_config_dict["provider_enable"] = parser["Scheduler"][
            "provider_enable"
        ]
        scheduler_config_dict["bitswap_enable"] = parser["Scheduler"]["bitswap_enable"]
        scheduler_config_dict["swarm_enable"] = parser["Scheduler"]["swarm_enable"]
        scheduler_config_dict["submit_delay"] = parser["Scheduler"]["submit_delay"]
        scheduler_config_dict["worker_pool"] = parser["Scheduler"]["worker_pool"]
        scheduler_config_dict["shutdown_delay"] = parser["Scheduler"]["shutdown_delay"]
        scheduler_config_dict["wait_before_startup"] = parser["Scheduler"][
            "wait_before_startup"
        ]
        scheduler_config_dict["log_file"] = parser["Scheduler"]["log_file"]

    except KeyError:
        parser["Scheduler"] = {}
        parser["Scheduler"]["beacon_enable"] = "True"
        parser["Scheduler"]["provider_enable"] = "True"
        parser["Scheduler"]["bitswap_enable"] = "True"
        parser["Scheduler"]["swarm_enable"] = "True"
        parser["Scheduler"]["submit_delay"] = "0"
        parser["Scheduler"]["worker_pool"] = "9"
        parser["Scheduler"]["shutdown_delay"] = "0"
        parser["Scheduler"]["wait_before_startup"] = "0"
        parser["Scheduler"]["log_file"] = "scheduler.log"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        scheduler_config_dict = {}
        scheduler_config_dict["beacon_enable"] = parser["Scheduler"]["beacon_enable"]
        scheduler_config_dict["provider_enable"] = parser["Scheduler"][
            "provider_enable"
        ]
        scheduler_config_dict["bitswap_enable"] = parser["Scheduler"]["bitswap_enable"]
        scheduler_config_dict["swarm_enable"] = parser["Scheduler"]["swarm_enable"]
        scheduler_config_dict["submit_delay"] = parser["Scheduler"]["submit_delay"]
        scheduler_config_dict["worker_pool"] = parser["Scheduler"]["worker_pool"]
        scheduler_config_dict["shutdown_delay"] = parser["Scheduler"]["shutdown_delay"]
        scheduler_config_dict["wait_before_startup"] = parser["Scheduler"][
            "wait_before_startup"
        ]
        scheduler_config_dict["log_file"] = parser["Scheduler"]["log_file"]

    return scheduler_config_dict


def get_clean_up_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        clean_up_config_dict = {}
        clean_up_config_dict["days_to_delay"] = parser["Clean_Up"]["days_to_delay"]
        clean_up_config_dict["sql_timeout"] = parser["Clean_Up"]["sql_timeout"]
        clean_up_config_dict["log_file"] = parser["Clean_Up"]["log_file"]
        clean_up_config_dict["connect_retries"] = parser["Clean_Up"]["connect_retries"]
        clean_up_config_dict["connect_retry_delay"] = parser["Clean_Up"][
            "connect_retry_delay"
        ]

    except KeyError:
        parser["Clean_Up"] = {}
        parser["Clean_Up"]["days_to_delay"] = "1"
        parser["Clean_Up"]["sql_timeout"] = "60"
        parser["Clean_Up"]["log_file"] = "clean_up.log"
        parser["Clean_Up"]["connect_retries"] = "30"
        parser["Clean_Up"]["connect_retry_delay"] = "10"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        clean_up_config_dict = {}
        clean_up_config_dict["days_to_delay"] = parser["Clean_Up"]["days_to_delay"]
        clean_up_config_dict["sql_timeout"] = parser["Clean_Up"]["sql_timeout"]
        clean_up_config_dict["log_file"] = parser["Clean_Up"]["log_file"]
        clean_up_config_dict["connect_retries"] = parser["Clean_Up"]["connect_retries"]
        clean_up_config_dict["connect_retry_delay"] = parser["Clean_Up"][
            "connect_retry_delay"
        ]

    return clean_up_config_dict


def get_ipfs_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        ipfs_config_dict = {}
        ipfs_config_dict["agent"] = parser["IPFS"]["agent"]
        ipfs_config_dict["sql_timeout"] = parser["IPFS"]["sql_timeout"]
        ipfs_config_dict["log_file"] = parser["IPFS"]["log_file"]
        ipfs_config_dict["connect_retries"] = parser["IPFS"]["connect_retries"]
        ipfs_config_dict["connect_retry_delay"] = parser["IPFS"]["connect_retry_delay"]

    except KeyError:
        url_dict = get_url_dict()
        i = 0
        not_found = True
        while i < 30 and not_found:
            try:
                with requests.post(url_dict["id"], stream=False) as r:
                    json_dict = json.loads(r.text)
                    not_found = False
            except ConnectionError:
                sleep(10)
                i += 1

        parser["IPFS"] = {}
        parser["IPFS"]["agent"] = json_dict["AgentVersion"]
        parser["IPFS"]["sql_timeout"] = "60"
        parser["IPFS"]["log_file"] = "ipfs.log"
        parser["IPFS"]["connect_retries"] = "30"
        parser["IPFS"]["connect_retry_delay"] = "10"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        ipfs_config_dict = {}
        ipfs_config_dict["agent"] = parser["IPFS"]["agent"]
        ipfs_config_dict["log_file"] = parser["IPFS"]["log_file"]
        ipfs_config_dict["sql_timeout"] = parser["IPFS"]["sql_timeout"]
        ipfs_config_dict["connect_retries"] = parser["IPFS"]["connect_retries"]
        ipfs_config_dict["connect_retry_delay"] = parser["IPFS"]["connect_retry_delay"]

    return ipfs_config_dict


def get_capture_peer_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        capture_peer_config_dict = {}
        capture_peer_config_dict["capture_interval_delay"] = parser["Capture_Peer"][
            "capture_interval_delay"
        ]
        capture_peer_config_dict["sql_timeout"] = parser["Capture_Peer"]["sql_timeout"]
        capture_peer_config_dict["wait_before_startup"] = parser["Capture_Peer"][
            "wait_before_startup"
        ]
        capture_peer_config_dict["max_intervals"] = parser["Capture_Peer"][
            "max_intervals"
        ]
        capture_peer_config_dict["shutdown_time"] = parser["Capture_Peer"][
            "shutdown_time"
        ]
        capture_peer_config_dict["q_server_port"] = parser["Capture_Peer"][
            "q_server_port"
        ]
        capture_peer_config_dict["log_file"] = parser["Capture_Peer"]["log_file"]
        capture_peer_config_dict["connect_retries"] = parser["Capture_Peer"][
            "connect_retries"
        ]
        capture_peer_config_dict["connect_retry_delay"] = parser["Capture_Peer"][
            "connect_retry_delay"
        ]

    except KeyError:
        parser["Capture_Peer"] = {}
        parser["Capture_Peer"]["capture_interval_delay"] = "600"
        parser["Capture_Peer"]["sql_timeout"] = "60"
        parser["Capture_Peer"]["wait_before_startup"] = "0"
        parser["Capture_Peer"]["max_intervals"] = "1440"
        parser["Capture_Peer"]["shutdown_time"] = "22:0:0"
        parser["Capture_Peer"]["q_server_port"] = "50000"
        parser["Capture_Peer"]["log_file"] = "capture_peer.log"
        parser["Capture_Peer"]["connect_retries"] = "30"
        parser["Capture_Peer"]["connect_retry_delay"] = "10"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        capture_peer_config_dict = {}

        capture_peer_config_dict["capture_interval_delay"] = parser["Capture_Peer"][
            "capture_interval_delay"
        ]
        capture_peer_config_dict["sql_timeout"] = parser["Capture_Peer"]["sql_timeout"]
        capture_peer_config_dict["wait_before_startup"] = parser["Capture_Peer"][
            "wait_before_startup"
        ]
        capture_peer_config_dict["max_intervals"] = parser["Capture_Peer"][
            "max_intervals"
        ]
        capture_peer_config_dict["shutdown_time"] = parser["Capture_Peer"][
            "shutdown_time"
        ]
        capture_peer_config_dict["q_server_port"] = parser["Capture_Peer"][
            "q_server_port"
        ]
        capture_peer_config_dict["log_file"] = parser["Capture_Peer"]["log_file"]
        capture_peer_config_dict["connect_retries"] = parser["Capture_Peer"][
            "connect_retries"
        ]
        capture_peer_config_dict["connect_retry_delay"] = parser["Capture_Peer"][
            "connect_retry_delay"
        ]

    return capture_peer_config_dict


def get_logger_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        logger_config_dict = {}
        logger_config_dict["default_level"] = parser["Logger"]["default_level"]
        logger_config_dict["console_level"] = parser["Logger"]["console_level"]
        logger_config_dict["file_level"] = parser["Logger"]["file_level"]

    except KeyError:
        parser["Logger"] = {}
        parser["Logger"]["default_level"] = "DEBUG"
        parser["Logger"]["console_level"] = "INFO"
        parser["Logger"]["file_level"] = "DEBUG"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        logger_config_dict = {}
        logger_config_dict["default_level"] = parser["Logger"]["default_level"]
        logger_config_dict["console_level"] = parser["Logger"]["console_level"]
        logger_config_dict["file_level"] = parser["Logger"]["file_level"]

    return logger_config_dict


def get_logger_server_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        logger_server_config_dict = {}
        logger_server_config_dict["log_file"] = parser["Logger_Server"]["log_file"]
        logger_server_config_dict["default_level"] = parser["Logger_Server"][
            "default_level"
        ]
        logger_server_config_dict["console_level"] = parser["Logger_Server"][
            "console_level"
        ]
        logger_server_config_dict["file_level"] = parser["Logger_Server"]["file_level"]

    except KeyError:
        parser["Logger_Server"] = {}
        parser["Logger_Server"]["log_file"] = "server.log"
        parser["Logger_Server"]["default_level"] = "DEBUG"
        parser["Logger_Server"]["console_level"] = "INFO"
        parser["Logger_Server"]["file_level"] = "DEBUG"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        logger_server_config_dict = {}
        logger_server_config_dict["log_file"] = parser["Logger_Server"]["log_file"]
        logger_server_config_dict["default_level"] = parser["Logger_Server"][
            "default_level"
        ]
        logger_server_config_dict["console_level"] = parser["Logger_Server"][
            "console_level"
        ]
        logger_server_config_dict["file_level"] = parser["Logger_Server"]["file_level"]

    return logger_server_config_dict


def get_want_list_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        want_list_config_dict = {}
        want_list_config_dict["wait_for_new_peer_minutes"] = parser["Want_List"][
            "wait_for_new_peer_minutes"
        ]
        want_list_config_dict["provider_zero_sample_count"] = parser["Want_List"][
            "provider_zero_sample_count"
        ]
        want_list_config_dict["provider_pool_workers"] = parser["Want_List"][
            "provider_pool_workers"
        ]
        want_list_config_dict["provider_maxtasks"] = parser["Want_List"][
            "provider_maxtasks"
        ]
        want_list_config_dict["bitswap_zero_sample_count"] = parser["Want_List"][
            "bitswap_zero_sample_count"
        ]
        want_list_config_dict["bitswap_pool_workers"] = parser["Want_List"][
            "bitswap_pool_workers"
        ]
        want_list_config_dict["bitswap_maxtasks"] = parser["Want_List"][
            "bitswap_maxtasks"
        ]
        want_list_config_dict["swarm_zero_sample_count"] = parser["Want_List"][
            "swarm_zero_sample_count"
        ]
        want_list_config_dict["swarm_pool_workers"] = parser["Want_List"][
            "swarm_pool_workers"
        ]
        want_list_config_dict["swarm_maxtasks"] = parser["Want_List"]["swarm_maxtasks"]
        want_list_config_dict["samples_per_minute"] = parser["Want_List"][
            "samples_per_minute"
        ]
        want_list_config_dict["number_of_samples_per_interval"] = parser["Want_List"][
            "number_of_samples_per_interval"
        ]
        want_list_config_dict["sql_timeout"] = parser["Want_List"]["sql_timeout"]
        want_list_config_dict["wait_before_startup"] = parser["Want_List"][
            "wait_before_startup"
        ]
        want_list_config_dict["max_intervals"] = parser["Want_List"]["max_intervals"]
        want_list_config_dict["shutdown_time"] = parser["Want_List"]["shutdown_time"]
        want_list_config_dict["q_server_port"] = parser["Want_List"]["q_server_port"]
        want_list_config_dict["server_log_file"] = parser["Want_List"][
            "server_log_file"
        ]
        want_list_config_dict["log_file"] = parser["Want_List"]["log_file"]
        want_list_config_dict["connect_retries"] = parser["Want_List"][
            "connect_retries"
        ]
        want_list_config_dict["connect_retry_delay"] = parser["Want_List"][
            "connect_retry_delay"
        ]

    except KeyError:
        parser["Want_List"] = {}
        parser["Want_List"]["wait_for_new_peer_minutes"] = "10"
        parser["Want_List"]["provider_zero_sample_count"] = "1440"
        parser["Want_List"]["provider_pool_workers"] = "3"
        parser["Want_List"]["provider_maxtasks"] = "1"
        parser["Want_List"]["bitswap_zero_sample_count"] = "5"
        parser["Want_List"]["bitswap_pool_workers"] = "3"
        parser["Want_List"]["bitswap_maxtasks"] = "1"
        parser["Want_List"]["swarm_zero_sample_count"] = "5"
        parser["Want_List"]["swarm_pool_workers"] = "20"
        parser["Want_List"]["swarm_maxtasks"] = "1"
        parser["Want_List"]["samples_per_minute"] = "6"
        parser["Want_List"]["number_of_samples_per_interval"] = "60"
        parser["Want_List"]["sql_timeout"] = "60"
        parser["Want_List"]["wait_before_startup"] = "30"
        parser["Want_List"]["max_intervals"] = "9999"
        parser["Want_List"]["shutdown_time"] = "99:99:99"
        parser["Want_List"]["q_server_port"] = "50000"
        parser["Want_List"]["server_log_file"] = "server.log"
        parser["Want_List"]["log_file"] = "want_list.log"
        parser["Want_List"]["connect_retries"] = "30"
        parser["Want_List"]["connect_retry_delay"] = "10"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        want_list_config_dict = {}
        want_list_config_dict["wait_for_new_peer_minutes"] = parser["Want_List"][
            "wait_for_new_peer_minutes"
        ]
        want_list_config_dict["provider_zero_sample_count"] = parser["Want_List"][
            "provider_zero_sample_count"
        ]
        want_list_config_dict["provider_pool_workers"] = parser["Want_List"][
            "provider_pool_workers"
        ]
        want_list_config_dict["provider_maxtasks"] = parser["Want_List"][
            "provider_maxtasks"
        ]
        want_list_config_dict["bitswap_zero_sample_count"] = parser["Want_List"][
            "bitswap_zero_sample_count"
        ]
        want_list_config_dict["bitswap_pool_workers"] = parser["Want_List"][
            "bitswap_pool_workers"
        ]
        want_list_config_dict["bitswap_maxtasks"] = parser["Want_List"][
            "bitswap_maxtasks"
        ]
        want_list_config_dict["swarm_zero_sample_count"] = parser["Want_List"][
            "swarm_zero_sample_count"
        ]
        want_list_config_dict["swarm_pool_workers"] = parser["Want_List"][
            "swarm_pool_workers"
        ]
        want_list_config_dict["swarm_maxtasks"] = parser["Want_List"]["swarm_maxtasks"]
        want_list_config_dict["samples_per_minute"] = parser["Want_List"][
            "samples_per_minute"
        ]
        want_list_config_dict["number_of_samples_per_interval"] = parser["Want_List"][
            "number_of_samples_per_interval"
        ]
        want_list_config_dict["sql_timeout"] = parser["Want_List"]["sql_timeout"]
        want_list_config_dict["wait_before_startup"] = parser["Want_List"][
            "wait_before_startup"
        ]
        want_list_config_dict["max_intervals"] = parser["Want_List"]["max_intervals"]
        want_list_config_dict["shutdown_time"] = parser["Want_List"]["shutdown_time"]
        want_list_config_dict["q_server_port"] = parser["Want_List"]["q_server_port"]
        want_list_config_dict["server_log_file"] = parser["Want_List"][
            "server_log_file"
        ]
        want_list_config_dict["log_file"] = parser["Want_List"]["log_file"]
        want_list_config_dict["connect_retries"] = parser["Want_List"][
            "connect_retries"
        ]
        want_list_config_dict["connect_retry_delay"] = parser["Want_List"][
            "connect_retry_delay"
        ]

    return want_list_config_dict


"""
def get_capture_bitswap_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        capture_bitswap_config_dict = {}
        capture_bitswap_config_dict["capture_interval_delay"] = parser[
            "Capture_Bitswap"
        ]["capture_interval_delay"]
        capture_bitswap_config_dict["sql_timeout"] = parser["Capture_Bitswap"][
            "sql_timeout"
        ]
        capture_bitswap_config_dict["wait_before_startup"] = parser["Capture_Bitswap"][
            "wait_before_startup"
        ]
        capture_bitswap_config_dict["max_intervals"] = parser["Capture_Bitswap"][
            "max_intervals"
        ]
        capture_bitswap_config_dict["shutdown_time"] = parser["Capture_Bitswap"][
            "shutdown_time"
        ]
        capture_bitswap_config_dict["log_file"] = parser["Capture_Bitswap"]["log_file"]
        capture_bitswap_config_dict["connect_retries"] = parser["Capture_Bitswap"][
            "connect_retries"
        ]
        capture_bitswap_config_dict["connect_retry_delay"] = parser["Capture_Bitswap"][
            "connect_retry_delay"
        ]

    except KeyError:
        parser["Capture_Bitswap"] = {}
        parser["Capture_Bitswap"]["capture_interval_delay"] = "600"
        parser["Capture_Bitswap"]["sql_timeout"] = "60"
        parser["Capture_Bitswap"]["wait_before_startup"] = "60"
        parser["Capture_Bitswap"]["max_intervals"] = "1440"
        parser["Capture_Bitswap"]["shutdown_time"] = "22:0:0"
        parser["Capture_Bitswap"]["log_file"] = "capture_bitswap.log"
        parser["Capture_Bitswap"]["connect_retries"] = "30"
        parser["Capture_Bitswap"]["connect_retry_delay"] = "30"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        capture_bitswap_config_dict = {}
        capture_bitswap_config_dict["capture_interval_delay"] = parser[
            "Capture_Bitswap"
        ]["capture_interval_delay"]
        capture_bitswap_config_dict["sql_timeout"] = parser["Capture_Bitswap"][
            "sql_timeout"
        ]
        capture_bitswap_config_dict["wait_before_startup"] = parser["Capture_Bitswap"][
            "wait_before_startup"
        ]
        capture_bitswap_config_dict["max_intervals"] = parser["Capture_Bitswap"][
            "max_intervals"
        ]
        capture_bitswap_config_dict["shutdown_time"] = parser["Capture_Bitswap"][
            "shutdown_time"
        ]
        capture_bitswap_config_dict["log_file"] = parser["Capture_Bitswap"]["log_file"]
        capture_bitswap_config_dict["connect_retries"] = parser["Capture_Bitswap"][
            "connect_retries"
        ]
        capture_bitswap_config_dict["connect_retry_delay"] = parser["Capture_Bitswap"][
            "connect_retry_delay"
        ]

    return capture_bitswap_config_dict


def get_capture_swarm_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        capture_swarm_config_dict = {}
        capture_swarm_config_dict["capture_interval_delay"] = parser["Capture_Swarm"][
            "capture_interval_delay"
        ]
        capture_swarm_config_dict["sql_timeout"] = parser["Capture_Swarm"][
            "sql_timeout"
        ]
        capture_swarm_config_dict["wait_before_startup"] = parser["Capture_Swarm"][
            "wait_before_startup"
        ]
        capture_swarm_config_dict["max_intervals"] = parser["Capture_Swarm"][
            "max_intervals"
        ]
        capture_swarm_config_dict["shutdown_time"] = parser["Capture_Swarm"][
            "shutdown_time"
        ]
        capture_swarm_config_dict["log_file"] = parser["Capture_Swarm"]["log_file"]
        capture_swarm_config_dict["connect_retries"] = parser["Capture_Swarm"][
            "connect_retries"
        ]
        capture_swarm_config_dict["connect_retry_delay"] = parser["Capture_Swarm"][
            "connect_retry_delay"
        ]

    except KeyError:
        parser["Capture_Swarm"] = {}
        parser["Capture_Swarm"]["capture_interval_delay"] = "600"
        parser["Capture_Swarm"]["sql_timeout"] = "60"
        parser["Capture_Swarm"]["wait_before_startup"] = "60"
        parser["Capture_Swarm"]["max_intervals"] = "1440"
        parser["Capture_Swarm"]["shutdown_time"] = "22:0:0"
        parser["Capture_Swarm"]["log_file"] = "capture_swarm.log"
        parser["Capture_Swarm"]["connect_retries"] = "30"
        parser["Capture_Swarm"]["connect_retry_delay"] = "30"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        capture_swarm_config_dict = {}
        capture_swarm_config_dict["capture_interval_delay"] = parser["Capture_Swarm"][
            "capture_interval_delay"
        ]
        capture_swarm_config_dict["sql_timeout"] = parser["Capture_Swarm"][
            "sql_timeout"
        ]
        capture_swarm_config_dict["wait_before_startup"] = parser["Capture_Swarm"][
            "wait_before_startup"
        ]
        capture_swarm_config_dict["max_intervals"] = parser["Capture_Swarm"][
            "max_intervals"
        ]
        capture_swarm_config_dict["shutdown_time"] = parser["Capture_Swarm"][
            "shutdown_time"
        ]
        capture_swarm_config_dict["log_file"] = parser["Capture_Swarm"]["log_file"]
        capture_swarm_config_dict["connect_retries"] = parser["Capture_Swarm"][
            "connect_retries"
        ]
        capture_swarm_config_dict["connect_retry_delay"] = parser["Capture_Swarm"][
            "connect_retry_delay"
        ]

    return capture_swarm_config_dict
    """


def get_queue_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        queue_config_dict = {}

        queue_config_dict["wait_before_startup"] = parser["Queue_Server"][
            "wait_before_startup"
        ]
        queue_config_dict["q_server_port"] = parser["Queue_Server"]["q_server_port"]

        queue_config_dict["log_file"] = parser["Queue_Server"]["log_file"]

    except KeyError:
        parser["Queue_Server"] = {}
        parser["Queue_Server"]["q_server_port"] = "50000"
        parser["Queue_Server"]["wait_before_startup"] = "0"

        parser["Queue_Server"]["log_file"] = "queue.log"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        queue_config_dict = {}

        queue_config_dict["wait_before_startup"] = parser["Queue_Server"][
            "wait_before_startup"
        ]
        queue_config_dict["q_server_port"] = parser["Queue_Server"]["q_server_port"]
        queue_config_dict["log_file"] = parser["Queue_Server"]["log_file"]

    return queue_config_dict
