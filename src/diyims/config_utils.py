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
    get_provider_capture_config_dict()
    get_want_list_config_dict()
    get_peer_table_maint_config_dict()
    get_publish_config_dict()
    get_peer_monitor_config_dict()
    get_clean_up_config_dict()
    get_metrics_config_dict()

    get_logger_config_dict()
    get_logger_server_config_dict()
    get_ipfs_config_dict()
    get_request_config_dict()

    get_db_init_config_dict()
    get_queue_config_dict()

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
        beacon_config_dict["wait_time"] = parser["Beacon"]["wait_time"]
        beacon_config_dict["beacon_length_seconds"] = parser["Beacon"][
            "beacon_length_seconds"
        ]
        beacon_config_dict["number_of_periods"] = parser["Beacon"]["number_of_periods"]
        beacon_config_dict["wait_before_startup"] = parser["Beacon"][
            "wait_before_startup"
        ]
        beacon_config_dict["max_intervals"] = parser["Beacon"]["max_intervals"]
        beacon_config_dict["shutdown_time"] = parser["Beacon"]["shutdown_time"]
        beacon_config_dict["q_server_port"] = parser["Beacon"]["q_server_port"]
        beacon_config_dict["sql_timeout"] = parser["Beacon"]["sql_timeout"]

        beacon_config_dict["connect_retries"] = parser["Beacon"]["connect_retries"]
        beacon_config_dict["connect_retry_delay"] = parser["Beacon"][
            "connect_retry_delay"
        ]
        beacon_config_dict["queues_enabled"] = parser["Beacon"]["queues_enabled"]
        beacon_config_dict["logging_enabled"] = parser["Beacon"]["logging_enabled"]
        beacon_config_dict["debug_enabled"] = parser["Beacon"]["debug_enabled"]

    except KeyError:
        parser["Beacon"] = {}
        parser["Beacon"]["wait_time"] = "30"
        parser["Beacon"]["beacon_length_seconds"] = "300"
        parser["Beacon"]["number_of_periods"] = "5"
        parser["Beacon"]["wait_before_startup"] = "0"
        parser["Beacon"]["max_intervals"] = "9999"
        parser["Beacon"]["shutdown_time"] = "99:99:99"
        parser["Beacon"]["q_server_port"] = "50000"
        parser["Beacon"]["sql_timeout"] = "60"
        parser["Beacon"]["connect_retries"] = "30"
        parser["Beacon"]["connect_retry_delay"] = "10"
        parser["Beacon"]["queues_enabled"] = "1"
        parser["Beacon"]["logging_enabled"] = "0"
        parser["Beacon"]["debug_enabled"] = "1"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        beacon_config_dict = {}
        beacon_config_dict["wait_time"] = parser["Beacon"]["wait_time"]
        beacon_config_dict["beacon_length_seconds"] = parser["Beacon"][
            "beacon_length_seconds"
        ]
        beacon_config_dict["number_of_periods"] = parser["Beacon"]["number_of_periods"]
        beacon_config_dict["wait_before_startup"] = parser["Beacon"][
            "wait_before_startup"
        ]
        beacon_config_dict["max_intervals"] = parser["Beacon"]["max_intervals"]
        beacon_config_dict["shutdown_time"] = parser["Beacon"]["shutdown_time"]
        beacon_config_dict["q_server_port"] = parser["Beacon"]["q_server_port"]
        beacon_config_dict["sql_timeout"] = parser["Beacon"]["sql_timeout"]
        beacon_config_dict["connect_retries"] = parser["Beacon"]["connect_retries"]
        beacon_config_dict["connect_retry_delay"] = parser["Beacon"][
            "connect_retry_delay"
        ]
        beacon_config_dict["queues_enabled"] = parser["Beacon"]["queues_enabled"]
        beacon_config_dict["logging_enabled"] = parser["Beacon"]["logging_enabled"]
        beacon_config_dict["debug_enabled"] = parser["Beacon"]["debug_enabled"]

    return beacon_config_dict


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
        scheduler_config_dict["reset_enable"] = parser["Scheduler"]["reset_enable"]
        scheduler_config_dict["metrics_enable"] = parser["Scheduler"]["metrics_enable"]
        scheduler_config_dict["beacon_enable"] = parser["Scheduler"]["beacon_enable"]
        scheduler_config_dict["provider_enable"] = parser["Scheduler"][
            "provider_enable"
        ]
        scheduler_config_dict["wantlist_enable"] = parser["Scheduler"][
            "wantlist_enable"
        ]
        scheduler_config_dict["publish_enable"] = parser["Scheduler"]["publish_enable"]
        scheduler_config_dict["peer_maint_enable"] = parser["Scheduler"][
            "peer_maint_enable"
        ]
        scheduler_config_dict["remote_monitor_enable"] = parser["Scheduler"][
            "remote_monitor_enable"
        ]
        scheduler_config_dict["bitswap_enable"] = parser["Scheduler"]["bitswap_enable"]
        scheduler_config_dict["swarm_enable"] = parser["Scheduler"]["swarm_enable"]
        scheduler_config_dict["submit_delay"] = parser["Scheduler"]["submit_delay"]
        scheduler_config_dict["worker_pool"] = parser["Scheduler"]["worker_pool"]
        scheduler_config_dict["shutdown_delay"] = parser["Scheduler"]["shutdown_delay"]
        scheduler_config_dict["wait_before_startup"] = parser["Scheduler"][
            "wait_before_startup"
        ]
        scheduler_config_dict["queues_enabled"] = parser["Scheduler"]["queues_enabled"]
        scheduler_config_dict["logging_enabled"] = parser["Scheduler"][
            "logging_enabled"
        ]
        scheduler_config_dict["debug_enabled"] = parser["Scheduler"]["debug_enabled"]

    except KeyError:
        parser["Scheduler"] = {}
        parser["Scheduler"]["reset_enable"] = "True"
        parser["Scheduler"]["metrics_enable"] = "True"
        parser["Scheduler"]["beacon_enable"] = "True"
        parser["Scheduler"]["provider_enable"] = "True"
        parser["Scheduler"]["wantlist_enable"] = "True"
        parser["Scheduler"]["publish_enable"] = "True"
        parser["Scheduler"]["peer_maint_enable"] = "True"
        parser["Scheduler"]["remote_monitor_enable"] = "True"
        parser["Scheduler"]["bitswap_enable"] = "True"
        parser["Scheduler"]["swarm_enable"] = "False"
        parser["Scheduler"]["submit_delay"] = "15"
        parser["Scheduler"]["worker_pool"] = "9"
        parser["Scheduler"]["shutdown_delay"] = "0"
        parser["Scheduler"]["wait_before_startup"] = "0"
        parser["Scheduler"]["queues_enabled"] = "1"
        parser["Scheduler"]["logging_enabled"] = "0"
        parser["Scheduler"]["debug_enabled"] = "1"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        scheduler_config_dict = {}

        scheduler_config_dict["reset_enable"] = parser["Scheduler"]["reset_enable"]
        scheduler_config_dict["metrics_enable"] = parser["Scheduler"]["metrics_enable"]
        scheduler_config_dict["beacon_enable"] = parser["Scheduler"]["beacon_enable"]
        scheduler_config_dict["provider_enable"] = parser["Scheduler"][
            "provider_enable"
        ]
        scheduler_config_dict["wantlist_enable"] = parser["Scheduler"][
            "wantlist_enable"
        ]
        scheduler_config_dict["publish_enable"] = parser["Scheduler"]["publish_enable"]
        scheduler_config_dict["peer_maint_enable"] = parser["Scheduler"][
            "peer_maint_enable"
        ]
        scheduler_config_dict["remote_monitor_enable"] = parser["Scheduler"][
            "remote_monitor_enable"
        ]
        scheduler_config_dict["bitswap_enable"] = parser["Scheduler"]["bitswap_enable"]
        scheduler_config_dict["swarm_enable"] = parser["Scheduler"]["swarm_enable"]
        scheduler_config_dict["submit_delay"] = parser["Scheduler"]["submit_delay"]
        scheduler_config_dict["worker_pool"] = parser["Scheduler"]["worker_pool"]
        scheduler_config_dict["shutdown_delay"] = parser["Scheduler"]["shutdown_delay"]
        scheduler_config_dict["wait_before_startup"] = parser["Scheduler"][
            "wait_before_startup"
        ]
        scheduler_config_dict["queues_enabled"] = parser["Scheduler"]["queues_enabled"]
        scheduler_config_dict["logging_enabled"] = parser["Scheduler"][
            "logging_enabled"
        ]
        scheduler_config_dict["debug_enabled"] = parser["Scheduler"]["debug_enabled"]

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
        clean_up_config_dict["hours_to_delay"] = parser["Clean_Up"]["hours_to_delay"]
        clean_up_config_dict["sql_timeout"] = parser["Clean_Up"]["sql_timeout"]
        clean_up_config_dict["log_file"] = parser["Clean_Up"]["log_file"]
        clean_up_config_dict["connect_retries"] = parser["Clean_Up"]["connect_retries"]
        clean_up_config_dict["connect_retry_delay"] = parser["Clean_Up"][
            "connect_retry_delay"
        ]

    except KeyError:
        parser["Clean_Up"] = {}
        parser["Clean_Up"]["hours_to_delay"] = "12"
        parser["Clean_Up"]["sql_timeout"] = "60"
        parser["Clean_Up"]["log_file"] = "clean_up.log"
        parser["Clean_Up"]["connect_retries"] = "30"
        parser["Clean_Up"]["connect_retry_delay"] = "10"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        clean_up_config_dict = {}
        clean_up_config_dict["hours_to_delay"] = parser["Clean_Up"]["hours_to_delay"]
        clean_up_config_dict["sql_timeout"] = parser["Clean_Up"]["sql_timeout"]
        clean_up_config_dict["log_file"] = parser["Clean_Up"]["log_file"]
        clean_up_config_dict["connect_retries"] = parser["Clean_Up"]["connect_retries"]
        clean_up_config_dict["connect_retry_delay"] = parser["Clean_Up"][
            "connect_retry_delay"
        ]

    return clean_up_config_dict


def get_shutdown_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        config_dict = {}
        config_dict["q_server_port"] = parser["Shutdown"]["q_server_port"]
        config_dict["sql_timeout"] = parser["Shutdown"]["sql_timeout"]
        config_dict["log_file"] = parser["Shutdown"]["log_file"]
        config_dict["connect_retries"] = parser["Shutdown"]["connect_retries"]
        config_dict["connect_retry_delay"] = parser["Shutdown"]["connect_retry_delay"]

    except KeyError:
        parser["Shutdown"] = {}
        parser["Shutdown"]["q_server_port"] = "50000"
        parser["Shutdown"]["sql_timeout"] = "60"
        parser["Shutdown"]["log_file"] = "Shutdown.log"
        parser["Shutdown"]["connect_retries"] = "30"
        parser["Shutdown"]["connect_retry_delay"] = "10"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        config_dict = {}
        config_dict["q_server_port"] = parser["Shutdown"]["q_server_port"]
        config_dict["sql_timeout"] = parser["Shutdown"]["sql_timeout"]
        config_dict["log_file"] = parser["Shutdown"]["log_file"]
        config_dict["connect_retries"] = parser["Shutdown"]["connect_retries"]
        config_dict["connect_retry_delay"] = parser["Shutdown"]["connect_retry_delay"]

    return config_dict


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
        ipfs_config_dict["stream"] = parser["IPFS"]["stream"]
        ipfs_config_dict["connect_timeout"] = parser["IPFS"]["connect_timeout"]
        ipfs_config_dict["read_timeout"] = parser["IPFS"]["read_timeout"]
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
                sleep(10)  # config value
                i += 1

        parser["IPFS"] = {}
        parser["IPFS"]["agent"] = json_dict["AgentVersion"]
        parser["IPFS"]["sql_timeout"] = "60"
        parser["IPFS"]["log_file"] = "ipfs.log"
        parser["IPFS"]["stream"] = "False"
        parser["IPFS"]["connect_timeout"] = "3.05"
        parser["IPFS"]["read_timeout"] = "120"
        parser["IPFS"]["connect_retries"] = "30"
        parser["IPFS"]["connect_retry_delay"] = "10"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        ipfs_config_dict = {}
        ipfs_config_dict["agent"] = parser["IPFS"]["agent"]
        ipfs_config_dict["log_file"] = parser["IPFS"]["log_file"]
        ipfs_config_dict["sql_timeout"] = parser["IPFS"]["sql_timeout"]
        ipfs_config_dict["stream"] = parser["IPFS"]["stream"]
        ipfs_config_dict["connect_timeout"] = parser["IPFS"]["connect_timeout"]
        ipfs_config_dict["read_timeout"] = parser["IPFS"]["read_timeout"]
        ipfs_config_dict["connect_retries"] = parser["IPFS"]["connect_retries"]
        ipfs_config_dict["connect_retry_delay"] = parser["IPFS"]["connect_retry_delay"]

    return ipfs_config_dict


def get_request_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        request_config_dict = {}
        request_config_dict["stream"] = parser["Request"]["stream"]
        request_config_dict["connect_timeout"] = parser["Request"]["connect_timeout"]
        request_config_dict["read_timeout"] = parser["Request"]["read_timeout"]
        request_config_dict["connect_retries"] = parser["Request"]["connect_retries"]
        request_config_dict["connect_retry_delay"] = parser["Request"][
            "connect_retry_delay"
        ]
        request_config_dict["request_retries"] = parser["Request"]["request_retries"]
        request_config_dict["request_retry_delay"] = parser["Request"][
            "request_retry_delay"
        ]
        request_config_dict["queues_enabled"] = parser["Request"]["queues_enabled"]
        request_config_dict["logging_enabled"] = parser["Request"]["logging_enabled"]
        request_config_dict["debug_enabled"] = parser["Request"]["debug_enabled"]

    except KeyError:
        parser["Request"] = {}
        parser["Request"]["stream"] = "False"
        parser["Request"]["connect_timeout"] = "3.05"
        parser["Request"]["read_timeout"] = "120"
        parser["Request"]["connect_retries"] = "30"
        parser["Request"]["connect_retry_delay"] = "10"
        parser["Request"]["request_retries"] = "2"
        parser["Request"]["request_retry_delay"] = "10"
        parser["Request"]["queues_enabled"] = "1"
        parser["Request"]["logging_enabled"] = "0"
        parser["Request"]["debug_enabled"] = "1"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        request_config_dict = {}
        request_config_dict["stream"] = parser["Request"]["stream"]
        request_config_dict["connect_timeout"] = parser["Request"]["connect_timeout"]
        request_config_dict["read_timeout"] = parser["Request"]["read_timeout"]
        request_config_dict["connect_retries"] = parser["Request"]["connect_retries"]
        request_config_dict["connect_retry_delay"] = parser["Request"][
            "connect_retry_delay"
        ]
        request_config_dict["request_retries"] = parser["Request"]["request_retries"]
        request_config_dict["request_retry_delay"] = parser["Request"][
            "request_retry_delay"
        ]
        request_config_dict["queues_enabled"] = parser["Request"]["queues_enabled"]
        request_config_dict["logging_enabled"] = parser["Request"]["logging_enabled"]
        request_config_dict["debug_enabled"] = parser["Request"]["debug_enabled"]

    return request_config_dict


def get_provider_capture_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        provider_capture_config_dict = {}
        provider_capture_config_dict["capture_interval_delay"] = parser[
            "Provider_Capture"
        ]["capture_interval_delay"]
        provider_capture_config_dict["sql_timeout"] = parser["Provider_Capture"][
            "sql_timeout"
        ]
        provider_capture_config_dict["wait_before_startup"] = parser[
            "Provider_Capture"
        ]["wait_before_startup"]
        provider_capture_config_dict["max_intervals"] = parser["Provider_Capture"][
            "max_intervals"
        ]
        provider_capture_config_dict["shutdown_time"] = parser["Provider_Capture"][
            "shutdown_time"
        ]
        provider_capture_config_dict["q_server_port"] = parser["Provider_Capture"][
            "q_server_port"
        ]
        provider_capture_config_dict["connect_retries"] = parser["Provider_Capture"][
            "connect_retries"
        ]
        provider_capture_config_dict["connect_retry_delay"] = parser[
            "Provider_Capture"
        ]["connect_retry_delay"]
        provider_capture_config_dict["queues_enabled"] = parser["Provider_Capture"][
            "queues_enabled"
        ]
        provider_capture_config_dict["logging_enabled"] = parser["Provider_Capture"][
            "logging_enabled"
        ]
        provider_capture_config_dict["debug_enabled"] = parser["Provider_Capture"][
            "debug_enabled"
        ]

    except KeyError:
        parser["Provider_Capture"] = {}
        parser["Provider_Capture"]["capture_interval_delay"] = "600"
        parser["Provider_Capture"]["sql_timeout"] = "60"
        parser["Provider_Capture"]["wait_before_startup"] = "0"
        parser["Provider_Capture"]["max_intervals"] = "9999"
        parser["Provider_Capture"]["shutdown_time"] = "99:99:99"
        parser["Provider_Capture"]["q_server_port"] = "50000"
        parser["Provider_Capture"]["connect_retries"] = "30"
        parser["Provider_Capture"]["connect_retry_delay"] = "10"
        parser["Provider_Capture"]["queues_enabled"] = "1"
        parser["Provider_Capture"]["logging_enabled"] = "0"
        parser["Provider_Capture"]["debug_enabled"] = "1"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        provider_capture_config_dict = {}

        provider_capture_config_dict["capture_interval_delay"] = parser[
            "Provider_Capture"
        ]["capture_interval_delay"]
        provider_capture_config_dict["sql_timeout"] = parser["Provider_Capture"][
            "sql_timeout"
        ]
        provider_capture_config_dict["wait_before_startup"] = parser[
            "Provider_Capture"
        ]["wait_before_startup"]
        provider_capture_config_dict["max_intervals"] = parser["Provider_Capture"][
            "max_intervals"
        ]
        provider_capture_config_dict["shutdown_time"] = parser["Provider_Capture"][
            "shutdown_time"
        ]
        provider_capture_config_dict["q_server_port"] = parser["Provider_Capture"][
            "q_server_port"
        ]
        provider_capture_config_dict["connect_retries"] = parser["Provider_Capture"][
            "connect_retries"
        ]
        provider_capture_config_dict["connect_retry_delay"] = parser[
            "Provider_Capture"
        ]["connect_retry_delay"]
        provider_capture_config_dict["queues_enabled"] = parser["Provider_Capture"][
            "queues_enabled"
        ]
        provider_capture_config_dict["logging_enabled"] = parser["Provider_Capture"][
            "logging_enabled"
        ]
        provider_capture_config_dict["debug_enabled"] = parser["Provider_Capture"][
            "debug_enabled"
        ]

    return provider_capture_config_dict


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
        want_list_config_dict["single_thread"] = parser["Want_List"]["single_thread"]
        want_list_config_dict["connect_retries"] = parser["Want_List"][
            "connect_retries"
        ]
        want_list_config_dict["connect_retry_delay"] = parser["Want_List"][
            "connect_retry_delay"
        ]
        want_list_config_dict["queues_enabled"] = parser["Want_List"]["queues_enabled"]
        want_list_config_dict["logging_enabled"] = parser["Want_List"][
            "logging_enabled"
        ]
        want_list_config_dict["debug_enabled"] = parser["Want_List"]["debug_enabled"]

    except KeyError:
        parser["Want_List"] = {}
        parser["Want_List"]["wait_for_new_peer_minutes"] = "10"
        parser["Want_List"]["provider_zero_sample_count"] = "9999"
        parser["Want_List"]["provider_pool_workers"] = "4"
        parser["Want_List"]["provider_maxtasks"] = "100"
        parser["Want_List"]["bitswap_zero_sample_count"] = "5"
        parser["Want_List"]["bitswap_pool_workers"] = "3"
        parser["Want_List"]["bitswap_maxtasks"] = "100"
        parser["Want_List"]["swarm_zero_sample_count"] = "5"
        parser["Want_List"]["swarm_pool_workers"] = "3"
        parser["Want_List"]["swarm_maxtasks"] = "100"
        parser["Want_List"]["samples_per_minute"] = "6"
        parser["Want_List"]["number_of_samples_per_interval"] = "60"
        parser["Want_List"]["sql_timeout"] = "60"
        parser["Want_List"]["wait_before_startup"] = "0"
        parser["Want_List"]["max_intervals"] = "9999"
        parser["Want_List"]["shutdown_time"] = "99:99:99"
        parser["Want_List"]["single_thread"] = "1"
        parser["Want_List"]["q_server_port"] = "50000"
        parser["Want_List"]["connect_retries"] = "30"
        parser["Want_List"]["connect_retry_delay"] = "10"
        parser["Want_List"]["queues_enabled"] = "1"
        parser["Want_List"]["logging_enabled"] = "0"
        parser["Want_List"]["debug_enabled"] = "1"

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
        want_list_config_dict["single_thread"] = parser["Want_List"]["single_thread"]
        want_list_config_dict["connect_retries"] = parser["Want_List"][
            "connect_retries"
        ]
        want_list_config_dict["connect_retry_delay"] = parser["Want_List"][
            "connect_retry_delay"
        ]
        want_list_config_dict["queues_enabled"] = parser["Want_List"]["queues_enabled"]
        want_list_config_dict["logging_enabled"] = parser["Want_List"][
            "logging_enabled"
        ]
        want_list_config_dict["debug_enabled"] = parser["Want_List"]["debug_enabled"]

    return want_list_config_dict


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


def get_db_init_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        db_init_config_dict = {}
        db_init_config_dict["sql_timeout"] = parser["DB_Init"]["sql_timeout"]
        db_init_config_dict["q_server_port"] = parser["DB_Init"]["q_server_port"]
        db_init_config_dict["connect_retries"] = parser["DB_Init"]["connect_retries"]
        db_init_config_dict["connect_retry_delay"] = parser["DB_Init"][
            "connect_retry_delay"
        ]
        db_init_config_dict["log_file"] = parser["DB_Init"]["log_file"]

    except KeyError:
        parser["DB_Init"] = {}
        parser["DB_Init"]["sql_timeout"] = "60"
        parser["DB_Init"]["q_server_port"] = "50000"
        parser["DB_Init"]["connect_retries"] = "30"
        parser["DB_Init"]["connect_retry_delay"] = "10"
        parser["DB_Init"]["log_file"] = "db_init.log"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        db_init_config_dict = {}
        db_init_config_dict["sql_timeout"] = parser["DB_Init"]["sql_timeout"]
        db_init_config_dict["q_server_port"] = parser["DB_Init"]["q_server_port"]
        db_init_config_dict["connect_retries"] = parser["DB_Init"]["connect_retries"]
        db_init_config_dict["connect_retry_delay"] = parser["DB_Init"][
            "connect_retry_delay"
        ]

        db_init_config_dict["log_file"] = parser["DB_Init"]["log_file"]

    return db_init_config_dict


def get_publish_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        publish_config_dict = {}
        publish_config_dict["wait_time"] = parser["Publish"]["wait_time"]
        publish_config_dict["sql_timeout"] = parser["Publish"]["sql_timeout"]
        publish_config_dict["wait_before_startup"] = parser["Publish"][
            "wait_before_startup"
        ]
        publish_config_dict["q_server_port"] = parser["Publish"]["q_server_port"]
        publish_config_dict["connect_retries"] = parser["Publish"]["connect_retries"]
        publish_config_dict["connect_retry_delay"] = parser["Publish"][
            "connect_retry_delay"
        ]
        publish_config_dict["queues_enabled"] = parser["Publish"]["queues_enabled"]
        publish_config_dict["logging_enabled"] = parser["Publish"]["logging_enabled"]
        publish_config_dict["debug_enabled"] = parser["Publish"]["debug_enabled"]

    except KeyError:
        parser["Publish"] = {}
        parser["Publish"]["wait_time"] = "600"
        parser["Publish"]["sql_timeout"] = "60"
        parser["Publish"]["wait_before_startup"] = "0"
        parser["Publish"]["q_server_port"] = "50000"
        parser["Publish"]["connect_retries"] = "30"
        parser["Publish"]["connect_retry_delay"] = "10"
        parser["Publish"]["queues_enabled"] = "1"
        parser["Publish"]["logging_enabled"] = "0"
        parser["Publish"]["debug_enabled"] = "1"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        publish_config_dict = {}
        publish_config_dict["wait_time"] = parser["Publish"]["wait_time"]
        publish_config_dict["sql_timeout"] = parser["Publish"]["sql_timeout"]
        publish_config_dict["wait_before_startup"] = parser["Publish"][
            "wait_before_startup"
        ]
        publish_config_dict["q_server_port"] = parser["Publish"]["q_server_port"]
        publish_config_dict["connect_retries"] = parser["Publish"]["connect_retries"]
        publish_config_dict["connect_retry_delay"] = parser["Publish"][
            "connect_retry_delay"
        ]

        publish_config_dict["queues_enabled"] = parser["Publish"]["queues_enabled"]
        publish_config_dict["logging_enabled"] = parser["Publish"]["logging_enabled"]
        publish_config_dict["debug_enabled"] = parser["Publish"]["debug_enabled"]

    return publish_config_dict


def get_metrics_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        metrics_config_dict = {}
        metrics_config_dict["wait_time"] = parser["Metrics"]["wait_time"]
        metrics_config_dict["wait_before_startup"] = parser["Metrics"][
            "wait_before_startup"
        ]
        metrics_config_dict["sql_timeout"] = parser["Metrics"]["sql_timeout"]
        metrics_config_dict["q_server_port"] = parser["Metrics"]["q_server_port"]
        metrics_config_dict["connect_retries"] = parser["Metrics"]["connect_retries"]
        metrics_config_dict["connect_retry_delay"] = parser["Metrics"][
            "connect_retry_delay"
        ]
        metrics_config_dict["queues_enabled"] = parser["Metrics"]["queues_enabled"]
        metrics_config_dict["logging_enabled"] = parser["Metrics"]["logging_enabled"]
        metrics_config_dict["debug_enabled"] = parser["Metrics"]["debug_enabled"]

    except KeyError:
        parser["Metrics"] = {}
        parser["Metrics"]["wait_time"] = "30"
        parser["Metrics"]["wait_before_startup"] = "0"
        parser["Metrics"]["sql_timeout"] = "60"
        parser["Metrics"]["q_server_port"] = "50000"
        parser["Metrics"]["connect_retries"] = "30"
        parser["Metrics"]["connect_retry_delay"] = "10"
        parser["Metrics"]["queues_enabled"] = "1"
        parser["Metrics"]["logging_enabled"] = "0"
        parser["Metrics"]["debug_enabled"] = "1"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        metrics_config_dict = {}
        metrics_config_dict["wait_time"] = parser["Metrics"]["wait_time"]
        metrics_config_dict["wait_before_startup"] = parser["Metrics"][
            "wait_before_startup"
        ]
        metrics_config_dict["sql_timeout"] = parser["Metrics"]["sql_timeout"]
        metrics_config_dict["q_server_port"] = parser["Metrics"]["q_server_port"]
        metrics_config_dict["connect_retries"] = parser["Metrics"]["connect_retries"]
        metrics_config_dict["connect_retry_delay"] = parser["Metrics"][
            "connect_retry_delay"
        ]
        metrics_config_dict["queues_enabled"] = parser["Metrics"]["queues_enabled"]
        metrics_config_dict["logging_enabled"] = parser["Metrics"]["logging_enabled"]
        metrics_config_dict["debug_enabled"] = parser["Metrics"]["debug_enabled"]

    return metrics_config_dict


def get_peer_table_maint_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        peer_table_maint_config_dict = {}
        peer_table_maint_config_dict["wait_time"] = parser["Peer_Table_Maintenance"][
            "wait_time"
        ]
        peer_table_maint_config_dict["wait_before_startup"] = parser[
            "Peer_Table_Maintenance"
        ]["wait_before_startup"]
        peer_table_maint_config_dict["sql_timeout"] = parser["Peer_Table_Maintenance"][
            "sql_timeout"
        ]
        peer_table_maint_config_dict["q_server_port"] = parser[
            "Peer_Table_Maintenance"
        ]["q_server_port"]
        peer_table_maint_config_dict["connect_retries"] = parser[
            "Peer_Table_Maintenance"
        ]["connect_retries"]
        peer_table_maint_config_dict["connect_retry_delay"] = parser[
            "Peer_Table_Maintenance"
        ]["connect_retry_delay"]
        peer_table_maint_config_dict["queues_enabled"] = parser[
            "Peer_Table_Maintenance"
        ]["queues_enabled"]
        peer_table_maint_config_dict["logging_enabled"] = parser[
            "Peer_Table_Maintenance"
        ]["logging_enabled"]
        peer_table_maint_config_dict["debug_enabled"] = parser[
            "Peer_Table_Maintenance"
        ]["debug_enabled"]

    except KeyError:
        parser["Peer_Table_Maintenance"] = {}
        parser["Peer_Table_Maintenance"]["wait_time"] = "600"
        parser["Peer_Table_Maintenance"]["wait_before_startup"] = "0"
        parser["Peer_Table_Maintenance"]["sql_timeout"] = "60"
        parser["Peer_Table_Maintenance"]["q_server_port"] = "50000"
        parser["Peer_Table_Maintenance"]["connect_retries"] = "30"
        parser["Peer_Table_Maintenance"]["connect_retry_delay"] = "10"
        parser["Peer_Table_Maintenance"]["queues_enabled"] = "1"
        parser["Peer_Table_Maintenance"]["logging_enabled"] = "0"
        parser["Peer_Table_Maintenance"]["debug_enabled"] = "1"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        peer_table_maint_config_dict = {}
        peer_table_maint_config_dict["wait_time"] = parser["Peer_Table_Maintenance"][
            "wait_time"
        ]
        peer_table_maint_config_dict["wait_before_startup"] = parser[
            "Peer_Table_Maintenance"
        ]["wait_before_startup"]
        peer_table_maint_config_dict["sql_timeout"] = parser["Peer_Table_Maintenance"][
            "sql_timeout"
        ]
        peer_table_maint_config_dict["q_server_port"] = parser[
            "Peer_Table_Maintenance"
        ]["q_server_port"]
        peer_table_maint_config_dict["connect_retries"] = parser[
            "Peer_Table_Maintenance"
        ]["connect_retries"]
        peer_table_maint_config_dict["connect_retry_delay"] = parser[
            "Peer_Table_Maintenance"
        ]["connect_retry_delay"]
        peer_table_maint_config_dict["queues_enabled"] = parser[
            "Peer_Table_Maintenance"
        ]["queues_enabled"]
        peer_table_maint_config_dict["logging_enabled"] = parser[
            "Peer_Table_Maintenance"
        ]["logging_enabled"]
        peer_table_maint_config_dict["debug_enabled"] = parser[
            "Peer_Table_Maintenance"
        ]["debug_enabled"]

    return peer_table_maint_config_dict


def get_peer_monitor_config_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    try:
        peer_monitor_config_dict = {}
        peer_monitor_config_dict["wait_time"] = parser["Peer_Monitor"]["wait_time"]
        peer_monitor_config_dict["beacon_length_seconds"] = parser["Peer_Monitor"][
            "beacon_length_seconds"
        ]
        peer_monitor_config_dict["number_of_periods"] = parser["Peer_Monitor"][
            "number_of_periods"
        ]
        peer_monitor_config_dict["wait_before_startup"] = parser["Peer_Monitor"][
            "wait_before_startup"
        ]
        peer_monitor_config_dict["max_intervals"] = parser["Peer_Monitor"][
            "max_intervals"
        ]
        peer_monitor_config_dict["shutdown_time"] = parser["Peer_Monitor"][
            "shutdown_time"
        ]
        peer_monitor_config_dict["q_server_port"] = parser["Peer_Monitor"][
            "q_server_port"
        ]
        peer_monitor_config_dict["sql_timeout"] = parser["Peer_Monitor"]["sql_timeout"]

        peer_monitor_config_dict["connect_retries"] = parser["Peer_Monitor"][
            "connect_retries"
        ]
        peer_monitor_config_dict["connect_retry_delay"] = parser["Peer_Monitor"][
            "connect_retry_delay"
        ]
        peer_monitor_config_dict["queues_enabled"] = parser["Peer_Monitor"][
            "queues_enabled"
        ]
        peer_monitor_config_dict["logging_enabled"] = parser["Peer_Monitor"][
            "logging_enabled"
        ]
        peer_monitor_config_dict["debug_enabled"] = parser["Peer_Monitor"][
            "debug_enabled"
        ]

    except KeyError:
        parser["Peer_Monitor"] = {}
        parser["Peer_Monitor"]["wait_time"] = "600"
        parser["Peer_Monitor"]["beacon_length_seconds"] = "300"
        parser["Peer_Monitor"]["number_of_periods"] = "5"
        parser["Peer_Monitor"]["wait_before_startup"] = "0"
        parser["Peer_Monitor"]["max_intervals"] = "9999"
        parser["Peer_Monitor"]["shutdown_time"] = "99:99:99"
        parser["Peer_Monitor"]["q_server_port"] = "50000"
        parser["Peer_Monitor"]["sql_timeout"] = "60"
        parser["Peer_Monitor"]["connect_retries"] = "30"
        parser["Peer_Monitor"]["connect_retry_delay"] = "10"
        parser["Peer_Monitor"]["queues_enabled"] = "1"
        parser["Peer_Monitor"]["logging_enabled"] = "0"
        parser["Peer_Monitor"]["debug_enabled"] = "1"

        with open(config_file, "w") as configfile:
            parser.write(configfile)

        peer_monitor_config_dict = {}
        peer_monitor_config_dict["wait_time"] = parser["Peer_Monitor"]["wait_time"]
        peer_monitor_config_dict["beacon_length_seconds"] = parser["Peer_Monitor"][
            "beacon_length_seconds"
        ]
        peer_monitor_config_dict["number_of_periods"] = parser["Peer_Monitor"][
            "number_of_periods"
        ]
        peer_monitor_config_dict["wait_before_startup"] = parser["Peer_Monitor"][
            "wait_before_startup"
        ]
        peer_monitor_config_dict["max_intervals"] = parser["Peer_Monitor"][
            "max_intervals"
        ]
        peer_monitor_config_dict["shutdown_time"] = parser["Peer_Monitor"][
            "shutdown_time"
        ]
        peer_monitor_config_dict["q_server_port"] = parser["Peer_Monitor"][
            "q_server_port"
        ]
        peer_monitor_config_dict["sql_timeout"] = parser["Peer_Monitor"]["sql_timeout"]
        peer_monitor_config_dict["connect_retries"] = parser["Peer_Monitor"][
            "connect_retries"
        ]
        peer_monitor_config_dict["connect_retry_delay"] = parser["Peer_Monitor"][
            "connect_retry_delay"
        ]
        peer_monitor_config_dict["queues_enabled"] = parser["Peer_Monitor"][
            "queues_enabled"
        ]
        peer_monitor_config_dict["logging_enabled"] = parser["Peer_Monitor"][
            "logging_enabled"
        ]
        peer_monitor_config_dict["debug_enabled"] = parser["Peer_Monitor"][
            "debug_enabled"
        ]

    return peer_monitor_config_dict
