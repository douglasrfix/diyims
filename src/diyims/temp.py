#from fastapi.encoders import jsonable_encoder
#from rich import print
import json
from pathlib import Path

#from sqlmodel import Session, col, create_engine, insert, select
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import Session

from diyims.path_utils import get_path_dict
from diyims.sqlmodels import Config


def test():
    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
    
    data = {}
    
    data['config_file'] = [
    {"section": "Common", "key": "sql_timeout", "value": "60", "comment": ""},
    {"section": "Common", "key": "wait_before_startup", "value": "0", "comment": ""},
    {"section": "Common", "key": "max_intervals", "value": "9999", "comment": ""},
    {"section": "Common", "key": "shutdown_time", "value": "99:99:99", "comment": ""},
    {"section": "Common", "key": "q_server_port", "value": "50000", "comment": ""},
    {"section": "Common", "key": "connect_retries", "value": "30", "comment": ""},
    {"section": "Common", "key": "connect_retry_delay", "value": "10", "comment": ""},
    {"section": "Common", "key": "connect_timeout", "value": "3.05", "comment": ""},
    {"section": "Common", "key": "queues_enabled", "value": "1", "comment": ""},
    {"section": "Common", "key": "logging_enabled", "value": "0", "comment": ""},
    {"section": "Common", "key": "debug_enabled", "value": "0", "comment": ""},
    {"section": "Common", "key": "connect_enabled", "value": "0", "comment": ""},
    {"section": "Common", "key": "component_test", "value": "0", "comment": ""},
    {"section": "Common", "key": "read_timeout", "value": "120", "comment": ""},
    {"section": "Provider_Capture", "key": "capture_interval_delay", "value": "600", "comment": ""},
    {"section": "Request", "key": "stream", "value": "False", "comment": ""},
    {"section": "Request", "key": "request_retries", "value": "2", "comment": ""},
    {"section": "Request", "key": "request_retry_delay", "value": "10", "comment": ""},
    {"section": "Beacon", "key": "wait_time", "value": "30", "comment": ""},
    {"section": "Beacon", "key": "beacon_length_seconds", "value": "300", "comment": ""},
    {"section": "Beacon", "key": "hours_to_delay", "value": "1", "comment": ""},
    {"section": "Beacon", "key": "number_of_periods", "value": "5", "comment": ""},
    {"section": "Scheduler", "key": "reset_enable", "value": "1", "comment": ""},
    {"section": "Scheduler", "key": "metrics_enable", "value": "1", "comment": ""},
    {"section": "Scheduler", "key": "beacon_enable", "value": "1", "comment": ""},
    {"section": "Scheduler", "key": "provider_enable", "value": "1", "comment": ""},
    {"section": "Scheduler", "key": "wantlist_enable", "value": "1", "comment": ""},
    {"section": "Scheduler", "key": "publish_enable", "value": "1", "comment": ""},
    {"section": "Scheduler", "key": "remote_monitor_enable", "value": "1", "comment": ""},
    {"section": "Scheduler", "key": "worker_pool", "value": "9", "comment": ""},
    {"section": "Scheduler", "key": "shutdown_delay", "value": "0", "comment": ""},
    {"section": "Scheduler", "key": "submit_delay", "value": "15", "comment": ""},
    {"section": "Scheduler", "key": "start_up_provide_enabled", "value": "1", "comment": ""},
    {"section": "Clean_Up", "key": "hours_to_delay", "value": "1", "comment": ""},
    {"section": "Clean_Up", "key": "log_file", "value": "clean_up.log", "comment": ""},
    {"section": "Shutdown", "key": "log_file", "value": "Shutdown.log", "comment": ""},
    {"section": "IPFS", "key": "log_file", "value": "ipfs.log", "comment": ""},
    {"section": "Logger", "key": "default_level", "value": "DEBUG", "comment": ""},
    {"section": "Logger", "key": "console_level", "value": "INFO", "comment": ""},
    {"section": "Logger", "key": "file_level", "value": "DEBUG", "comment": ""},
    {"section": "Logger_Server", "key": "log_file", "value": "server.log", "comment": ""},
    {"section": "Logger_Server", "key": "default_level", "value": "DEBUG", "comment": ""},
    {"section": "Logger_Server", "key": "console_level", "value": "INFO", "comment": ""},
    {"section": "Logger_Server", "key": "file_level", "value": "DEBUG", "comment": ""},
    {"section": "Want_List", "key": "wait_for_new_peer_minutes", "value": "10", "comment": ""},
    {"section": "Want_List", "key": "provider_zero_sample_count", "value": "5", "comment": ""},
    {"section": "Want_List", "key": "provider_pool_workers", "value": "4", "comment": ""},
    {"section": "Want_List", "key": "provider_maxtasks", "value": "100", "comment": ""},
    {"section": "Want_List", "key": "bitswap_zero_sample_count", "value": "5", "comment": ""},
    {"section": "Want_List", "key": "bitswap_pool_workers", "value": "3", "comment": ""},
    {"section": "Want_List", "key": "bitswap_maxtasks", "value": "100", "comment": ""},
    {"section": "Want_List", "key": "swarm_zero_sample_count", "value": "5", "comment": ""},
    {"section": "Want_List", "key": "swarm_pool_workers", "value": "3", "comment": ""},
    {"section": "Want_List", "key": "swarm_maxtasks", "value": "100", "comment": ""},
    {"section": "Want_List", "key": "samples_per_minute", "value": "6", "comment": ""},
    {"section": "Want_List", "key": "number_of_samples_per_interval", "value": "60", "comment": ""},
    {"section": "Want_List", "key": "single_thread", "value": "1", "comment": ""},
    {"section": "Queue_Server", "key": "log_file", "value": "queue.log", "comment": ""},
    {"section": "DB_Init", "key": "log_file", "value": "db_init.log", "comment": ""},
    {"section": "Publish", "key": "wait_time", "value": "600", "comment": ""},
    {"section": "Metrics", "key": "wait_time", "value": "30", "comment": ""},
    {"section": "Peer_Monitor", "key": "wait_time", "value": "600", "comment": ""},
    ]
    
    stmt = delete(Config)
    with Session(engine) as session:
        session.execute(stmt)
        session.commit()
    
    with Session(engine) as session:
        session.execute(insert(Config), data["config_file"])
        session.commit()
    
def complete_publish_config_table(call_stack: str):
    call_stack = call_stack + ":complete_publish_config_table"
    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
    file_name = 'config.tbl'
    in_file = Path(path_dict["update_path"]).joinpath(file_name)

    try:
        with open(in_file, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        print("Error: File not found.")
    except json.JSONDecodeError:
        print("Error: Invalid JSON format.")   

    stmt = delete(Config)
    with Session(engine) as session:
        session.execute(stmt)
        session.commit()
    
    with Session(engine) as session:
        session.execute(insert(Config), data["config_file"])
        session.commit()

if __name__ == "__main__":
    #freeze_support()
    #set_start_method("spawn")

    #test()
    complete_publish_config_table('cmd')