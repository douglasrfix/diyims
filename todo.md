
# TODO: > 0.0.0a0 can this interface support support batch, command line and
#   invoking from a windows application like file explorer?
# TODO: > 0.0.0a0 powershell variable $Error to see stderr output on the
#   console.
# TODO: create a minimum command interface that allows a file or files of the correct format
#       to ab added to the system correctly.

# TODO: error vs warning for permissive failures
# [ ]: add to the header header for announcing python version platform and ipfs agent
# [ ]: introduce logging
# [x]: for ipfs testing clean up
# [x]: todo checkboxes, colors in list
# [ ]: Add backup and recovery of test environment for testing. save and restore
# [x]: create 3.8.1 dev environment for flake8 support
# [x]: 0.0.0a0 test 3.8 and 3.9 for import lib behavior
# [ ]: 0.0.0a0 Verify installation behavior on a different platform (Linux).
# [x]: run full test suite against v22/3.8.1
# [x]: explore single function testing
# [x]: fix references to cartest .car file
# [x]: set hash only to false and pin to true
# [ ]: Data based ipfs clean up instead of brute for approach currently in use.
# [x]: Where does tox look for python interpreters for multi python testing?
# [x]: explore git enhancement
# [x]: refactor unsupported platform to enhance self documentation
# [x]: enhanced tox config to ease future python version testing
# [ ]: find providers of the network name and feed them to peer table maintenance
# [ ]: peer table maintenance ignores existing peers or negotiates with the peer to register
# [ ]: peer table maintenance adds new entries and updates existing entries from published tables or registrations
# [x]: rename import_lib to py_version_dep
# [ ]: create ipfs_version_dep place holder
# [ ]: create platform_dep place holder
# [ ]: refactor in line dictionary specification to include from a get_????_dict where ??? is the table name
# [ ]: evaluate pin roots value true false for real effect.  It doesn't seem to pin if true and it false to produce a response if #      its false. using ipfs help for each command that is used to detect when something changed
# [x]: split the issue into multiple steps to facilitate testing on multiple platforms to give me some peers to deal with.
# [x]: need a python version override to remove the --force for this test
# [x]: untested platform logic has moved and no longer supplies path dict in error message
    # remove from db_install
# [x]: remove untested error from db_init
# [x]: remove untested platform from header_ops
# [x]: need test for each value in unSupported list
# [x]: cleanup environ after use use monkeypatch
# [x]: need test for App not installed path
# [x]: need test for no override in get windows path
# [x]: Remove tox from 3.8.1
# [x]: create 9? environment for testing
# [x]: create process for posix (linux) paths
# [x]: create tests for posix paths
# [ ]: support posix mount points
# [ ]: support testing of posix mount points
# [x]: fix drive letter support
