from dataclasses import dataclass


@dataclass
class SetControlsReturn:
    """
    Structure to return the results for set_controls function.


    """

    queues_enabled: bool = None
    logging_enabled: bool = None
    component_test: bool = None
    debug_enabled: bool = None
    single_thread: bool = None
    metrics_enabled: bool = None


@dataclass
class WantlistCaptureProcessMainArgs:
    """
     _summary_

    _extended_summary_
    """

    call_stack: str
    want_list_config_dict: dict = None
    provider_peer_table_row: dict = None
    peer_type: str = None
    set_controls_return: SetControlsReturn = None


@dataclass
class SetSelfReturn:
    """
    Structure to return the results for set_controls function.


    """

    self: str
    IPNS_name: str
