import os
import platform
import sys

from diyims.error_classes import UnSupportedPlatformError


def test_os_platform():
    try:
        sys_platform = os.environ["OVERRIDE_PLATFORM"]

    except KeyError:
        sys_platform = sys.platform

    if sys_platform.startswith("freebsd"):
        print(
            "FreeBSD(a descendent of the Berkley Software Distribution) found and not tested"
        )
        raise (UnSupportedPlatformError(sys_platform))
    elif sys_platform.startswith("linux"):
        """Linux(a family of unix like environments using the Linux kernel from Linus Torvalds) found and not tested"""
        return sys_platform
    elif sys_platform.startswith("aix"):
        print("AIX(IBM Unix variant)  found and not supported")
        raise (UnSupportedPlatformError(sys_platform))
    elif sys_platform.startswith("wasi"):
        print("WASI(Web Assembly) found and not supported")
        raise (UnSupportedPlatformError(sys_platform))
    elif sys_platform.startswith("win32"):
        """win32 is valid for 32 and 64 bit systems"""
        try:
            sys_release = os.environ["OVERRIDE_RELEASE"]
        except KeyError:
            sys_release = platform.release()

        os_platform = sys_platform + ":" + sys_release
        return os_platform
    elif sys_platform.startswith("cygwin"):
        print("CYGWIN(Unix like environment for Windows) found and not supported")
        raise (UnSupportedPlatformError(sys_platform))
    elif sys_platform.startswith("darwin"):
        print("macOS found and not supported")
        raise (UnSupportedPlatformError(sys_platform))
    else:
        print("OS not identified and thus not supported")
        raise (UnSupportedPlatformError(sys_platform))


def get_python_version():
    """
    major, minor, micro, releaselevel, and serial. All values except releaselevel are integers;
    the release level is 'alpha', 'beta', 'candidate', or 'final'.
    The version_info value corresponding to the Python version 2.0 is (2, 0, 0, 'final', 0).
    The components can also be accessed by name, so sys.version_info[0] is equivalent to sys.version_info.major and so on.
    """

    python_version = (
        str(sys.version_info.major)
        + "."
        + str(sys.version_info.minor)
        + "."
        + str(sys.version_info.micro)
    )

    return python_version
