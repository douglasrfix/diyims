from dataclasses import dataclass


@dataclass
class SetControlsReturn:
    """
    Structure to return the results for set_controls function.


    """

    queues_enabled: bool = False
    logging_enabled: bool = False
    component_test: bool = False
    debug_enabled: bool = False
    single_thread: bool = False
    metrics_enabled: bool = False
    connect_enabled: bool = False


@dataclass
class WantlistCaptureProcessMainArgs:
    """
     _summary_

    _extended_summary_
    """

    call_stack: str
    want_list_config_dict: dict
    provider_peer_table_row: dict
    peer_type: str
    set_controls_return: SetControlsReturn


@dataclass
class SetSelfReturn:
    """
    Structure to return the results for set_controls function.


    """

    self: str
    IPNS_name: str
