
# TODO: > 0.0.0a0 can this interface support support batch, command line and invoking from a windows application like file explorer?

## TODO: > 0.0.0a0 powershell variable $Error to see stderr output on the console

## TODO: create a minimum command interface that allows a file or files of the correct format to ab added to the system correctly

## TODO: error vs warning for permissive failures

## TODO: add shutdown functionality by command line

## TODO: add restart capability by resetting peer processing status at startup

## [x]: introduce logging

## [x]: for ipfs testing clean up

## [x]: todo checkboxes, colors in list

## [ ]: Add backup and recovery of test environment for testing. save and restore

## [x]: create 3.8.1 dev environment for flake8 support

## [x]: 0.0.0a0 test 3.8 and 3.9 for import lib behavior

## [ ]: 0.0.0a0 Verify installation behavior on a different platform (Linux) need install logic

## [x]: run full test suite against v22/3_8_1

## [x]: explore single function testing

## [x]: fix references to cartest .car file

## [x]: set hash only to false and pin to true

## [x]: Data based ipfs clean up instead of brute for approach currently in use

## [x]: Where does tox look for python interpreters for multi python testing?

## [x]: explore git enhancement

## [x]: refactor unsupported platform to enhance self documentation

## [x]: enhanced tox config to ease future python version testing

## [x]: find providers of the network name and feed them to peer table maintenance

## [x]: peer table maintenance ignores existing peers or negotiates with the peer to register

## [x]: peer table maintenance adds new entries and updates existing entries from published tables or registrations

## [x]: rename import_lib to py_version_dep

## [ ]: create ipfs_version_dep place holder

## [ ]: create platform_dep place holder

## [ ]: refactor in line dictionary specification to include from a get_????_dict where ??? is the table name

## [x]: evaluate pin roots value true false for real effect.  It doesn't seem to pin if true and it false to produce a response if its false. using ipfs help for each command that is used to detect when something changed

## [x]: split the issue into multiple steps to facilitate testing on multiple platforms to give me some peers to deal with

## [x]: need a python version override to remove the --force for this test

## [x]: untested platform logic has moved and no longer supplies path dict in error message remove from db_install

## [x]: remove untested error from db_init

## [x]: remove untested platform from header_ops

## [x]: need test for each value in unSupported list

## [x]: cleanup environ after use use monkeypatch

## [x]: need test for App not installed path

## [x]: need test for no override in get windows path

## [x]: Remove tox from 3.8.1

## [x]: create 9? environment for testing

## [x]: create process for posix (linux) paths

## [x]: create tests for posix paths

## [ ]: support posix mount points

## [ ]: support testing of posix mount points

## [x]: fix drive letter support

## [x]: create want list table

## [x]: standardize db name capitalization

## [ ]: need a db reset to previous condition function

## [ ]: rename header to announce

## [ ]: CLI utility to create and get non-existent CIDs for negotiation testing both static CIDs and dynamic

## [x]: add to the peer table for announcing python version platform and ipfs agent

## [x]: add peer status to peer table and delete seq number

## [ ]: support in-place db schema changes

## [x]: add diyims agent to identify who/what created an artifact for the network

## [x]: Set PYTHONDEVMODE to 1 for running tests

## [x]: add table value for platform release level eg. window 10

## [x]: add peer type to peer table

## [x]: combine sql_table_dict with db utils rename db_operations

## [x]: apt requires support is > 9.0

## [x]: create config entry for want item.json

## [x]: make unique file name

## [x]: this will require a purge function

## [x]: create config entry for want item

## [x]: for beacon timings

## [ ]: should cache reside on data drive if one is specified?

## [ ]: should state reside on data drive if one is specified?

## [x]: add date attributes to selection for purge

## [x]: add wait for ipfs function

## [x]: get time to shut sown from config

## [x]: add number of intervals to the shutdown criteria

## []: add scheduling element to peer table

## []: add provider monitoring data element to peer table

## NOTE: task to clean up beacon CIDs

## [x]: #7 support for peer table revision ABANDONED

## [x]: #17 up grade db-init process to use new header functions

## TODO: #19 implement rapid shut down

## [x]: #21 integrate metrics update into start up process

## [x]: #22 create a process to periodically query a peer ipns name

## [x]: #27 Beacon fails at 101

## [x]: #29 remove excess logging in normal operations and remove requirement to change config

## [x]: #31 expand range of cleanup scope and provide internal management of runtimes

## [x]: #35 add peer packaging to filter process

## [x]: #37 refresh peer address if it changes during peer capture

## TODO: #39 validate signature when adding a local peer entry via chain maint
