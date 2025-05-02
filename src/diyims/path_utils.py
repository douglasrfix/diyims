"""
This module provides the knowledge to navigate the installation paths used by
the various supported platforms.

The primary challenge to the developer is the lack of real standards. Most of
traditional placement of files is based upon historical conventions and
practices established early in the history of the platform and even the
computing industry itself.

These standards were not intended for single user machines with no
administrative support. With this in mind, and through the luck of the search
engine draw, I have modeled my placement based upon a go implementation of the
XDG specification. (https://pkg.go.dev/github.com/adrg/xdg)

Given the lack of a convenient method of detecting the version of windows,
the choice between %AppData% and %LocalAppData% was in favor %AppData% for
legacy versions support.

All of the files are placed in user spaces to minimize permission issues.
"""

import configparser
import os
from pathlib import Path

from diyims.error_classes import ApplicationNotInstalledError
from diyims.platform_utils import test_os_platform


def get_install_template_dict():
    os_platform = test_os_platform()

    if os_platform.startswith("win32"):
        install_template_dict = get_win32_template_dict()

    elif os_platform.startswith("linux"):
        install_template_dict = get_linux_template_dict()

    return install_template_dict


def get_path_dict():
    install_dict = get_install_template_dict()

    config_file = Path().joinpath(install_dict["config_path"], "diyims.ini")
    parser = configparser.ConfigParser()

    try:
        with open(config_file, "r") as configfile:
            parser.read_file(configfile)

    except FileNotFoundError:
        raise ApplicationNotInstalledError(" ")

    path_dict = {}
    path_dict["config_path"] = Path(parser["Paths"]["config_path"])
    path_dict["config_file"] = Path(parser["Files"]["config_file"])
    path_dict["db_path"] = Path(parser["Paths"]["db_path"])
    path_dict["db_file"] = Path(parser["Files"]["db_file"])
    path_dict["log_path"] = Path(parser["Paths"]["log_path"])
    path_dict["header_path"] = Path(parser["Paths"]["header_path"])
    path_dict["header_file"] = Path(parser["Files"]["header_file"])
    path_dict["peer_path"] = Path(parser["Paths"]["peer_path"])
    path_dict["peer_file"] = Path(parser["Files"]["peer_file"])
    path_dict["want_item_path"] = Path(parser["Paths"]["want_item_path"])
    path_dict["want_item_file"] = Path(parser["Files"]["want_item_file"])
    path_dict["sign_path"] = Path(parser["Paths"]["sign_path"])
    path_dict["sign_file"] = Path(parser["Files"]["sign_file"])
    return path_dict


def get_linux_template_dict():
    try:
        xdg_home = Path(os.environ["OVERRIDE_HOME"])

    except KeyError:
        xdg_home = Path.home()

    xdg_data_home = Path(xdg_home).joinpath(".local", "diyims", "share")
    xdg_config_home = Path(xdg_home).joinpath(".config", "diyims")
    xdg_cache_home = Path(xdg_home).joinpath(".cache", "diyims")
    xdg_state_home = Path(xdg_home).joinpath(".local", "diyims", "state")

    template_path_dict = {}
    template_path_dict["config_path"] = xdg_config_home
    template_path_dict["db_path"] = xdg_data_home
    template_path_dict["log_path"] = xdg_state_home
    template_path_dict["header_path"] = xdg_cache_home
    template_path_dict["peer_path"] = xdg_cache_home
    template_path_dict["want_item_path"] = xdg_cache_home
    template_path_dict["sign_path"] = xdg_cache_home

    return template_path_dict


def get_win32_template_dict():
    try:
        xdg_home = Path(os.environ["OVERRIDE_HOME"])
    except KeyError:
        xdg_home = Path(os.environ["UserProfile"])

    try:
        Roaming = str(os.environ["ROAMING"])
    except KeyError:
        Roaming = "Roaming"

    xdg_data_home = Path(xdg_home).joinpath("AppData", Roaming, "diyims", "Data")
    xdg_config_home = Path(xdg_home).joinpath("AppData", Roaming, "diyims", "Config")
    xdg_cache_home = Path(xdg_home).joinpath("AppData", Roaming, "diyims", "Cache")
    xdg_state_home = Path(xdg_home).joinpath("AppData", Roaming, "diyims", "State")

    template_path_dict = {}
    template_path_dict["config_path"] = xdg_config_home
    template_path_dict["db_path"] = xdg_data_home
    template_path_dict["log_path"] = xdg_state_home
    template_path_dict["header_path"] = xdg_cache_home
    template_path_dict["peer_path"] = xdg_cache_home
    template_path_dict["want_item_path"] = xdg_cache_home
    template_path_dict["sign_path"] = xdg_cache_home

    return template_path_dict


def get_unique_item_file(want_item_path, proto_item_file):
    proto_item_pattern = proto_item_file.stem + "{:05d}" + proto_item_file.suffix
    counter = 0
    while True:
        counter += 1
        file_path = Path(want_item_path).joinpath(proto_item_pattern.format(counter))
        if not file_path.exists():
            return file_path
