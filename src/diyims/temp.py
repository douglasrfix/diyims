#from fastapi.encoders import jsonable_encoder
from rich import print

#from sqlmodel import Session, col, create_engine, insert, select
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import Session

from diyims.path_utils import get_path_dict
from diyims.sqlmodels import Config


def test(section: str):
    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
    
    override_dict = {}
    common_dict = {}
    data = {}
    
    data['config_file'] = [
    {"section": "Common", "key": "sql_timeout", "value": "60"},
    {"section": "Common", "key": "wait_before_startup", "value": "0"},
    {"section": "Common", "key": "max_intervals", "value": "9999"},
    {"section": "Common", "key": "shutdown_time", "value": "99:99:99"},
    {"section": "Common", "key": "q_server_port", "value": "50000"},
    {"section": "Common", "key": "connect_retries", "value": "30"},
    {"section": "Common", "key": "connect_retry_delay", "value": "10"},
    {"section": "Common", "key": "connect_timeout", "value": "3.05"},
    {"section": "Common", "key": "queues_enabled", "value": "1"},
    {"section": "Common", "key": "logging_enabled", "value": "0"},
    {"section": "Common", "key": "debug_enabled", "value": "0"},
    {"section": "Common", "key": "connect_enabled", "value": "0"},
    {"section": "Common", "key": "component_test", "value": "0"},
    {"section": "Provider_Capture", "key": "capture_interval_delay", "value": "600"},
    #{"section": "Common", "key": "stream", "value": "False"},
    {"section": "Common", "key": "read_timeout", "value": "120"},
    {"section": "Request", "key": "request_retries", "value": "2"},
    {"section": "Request", "key": "request_retry_delay", "value": "10"},
    {"section": "Beacon", "key": "wait_time", "value": "30"},
    {"section": "Beacon", "key": "beacon_length_seconds", "value": "300"},
    {"section": "Beacon", "key": "number_of_periods", "value": "5"},
    {"section": "Scheduler", "key": "reset_enable", "value": "1"},
    {"section": "Scheduler", "key": "metrics_enable", "value": "1"},
    {"section": "Scheduler", "key": "beacon_enable", "value": "1"},
    {"section": "Scheduler", "key": "provider_enable", "value": "1"},
    {"section": "Scheduler", "key": "wantlist_enable", "value": "1"},
    {"section": "Scheduler", "key": "publish_enable", "value": "1"},
    {"section": "Scheduler", "key": "peer_maint_enable", "value": "0"},
    {"section": "Scheduler", "key": "remote_monitor_enable", "value": "1"},
    {"section": "Scheduler", "key": "worker_pool", "value": "9"},
    {"section": "Scheduler", "key": "shutdown_delay", "value": "0"},
    {"section": "Scheduler", "key": "submit_delay", "value": "15"},
    {"section": "Clean_Up", "key": "hours_to_delay", "value": "1"},
    {"section": "Clean_Up", "key": "log_file", "value": "clean_up.log"},
    {"section": "Shutdown", "key": "log_file", "value": "Shutdown.log"},
    {"section": "IPFS", "key": "log_file", "value": "ipfs.log"},
    {"section": "Logger", "key": "default_level", "value": "DEBUG"},
    {"section": "Logger", "key": "console_level", "value": "INFO"},
    {"section": "Logger", "key": "file_level", "value": "DEBUG"},
    {"section": "Logger_Server", "key": "log_file", "value": "server.log"},
    {"section": "Logger_Server", "key": "default_level", "value": "DEBUG"},
    {"section": "Logger_Server", "key": "console_level", "value": "INFO"},
    {"section": "Logger_Server", "key": "file_level", "value": "DEBUG"},
    {"section": "Want_List", "key": "wait_for_new_peer_minutes", "value": "10"},
    {"section": "Want_List", "key": "provider_zero_sample_count", "value": "5"},
    {"section": "Want_List", "key": "provider_pool_workers", "value": "4"},
    {"section": "Want_List", "key": "provider_maxtasks", "value": "100"},
    {"section": "Want_List", "key": "bitswap_zero_sample_count", "value": "5"},
    {"section": "Want_List", "key": "bitswap_pool_workers", "value": "3"},
    {"section": "Want_List", "key": "bitswap_maxtasks", "value": "100"},
    {"section": "Want_List", "key": "swarm_zero_sample_count", "value": "5"},
    {"section": "Want_List", "key": "swarm_pool_workers", "value": "3"},
    {"section": "Want_List", "key": "swarm_maxtasks", "value": "100"},
    {"section": "Want_List", "key": "samples_per_minute", "value": "6"},
    {"section": "Want_List", "key": "number_of_samples_per_interval", "value": "60"},
    {"section": "Want_List", "key": "single_thread", "value": "1"},
    {"section": "Queue_Server", "key": "log_file", "value": "queue.log"},
    {"section": "DB_Init", "key": "log_file", "value": "db_init.log"},
    {"section": "Publish", "key": "wait_time", "value": "600"},
    {"section": "Metrics", "key": "wait_time", "value": "30"},
    {"section": "Peer_Monitor", "key": "wait_time", "value": "600"},
    {"section": "Peer_Monitor", "key": "beacon_length_seconds", "value": "300"},
    {"section": "Peer_Monitor", "key": "number_of_periods", "value": "5"},
    ]
    
    stmt = delete(Config)
    with Session(engine) as session:
        session.execute(stmt)
        session.commit()
    
    with Session(engine) as session:
        session.execute(insert(Config), data["config_file"])
        session.commit()   

     
    with Session(engine) as session:
        result_dict = session.execute(Config.__table__.select()).mappings().all()       

    for db_dict in result_dict:

        if (db_dict['section'] == 'Common'):
            
            common_dict[db_dict['key']] = db_dict['value']

        elif (db_dict['section'] == section):

            override_dict[db_dict['key']] = db_dict['value']
    
    final_dict = {**common_dict, **override_dict}
    return final_dict
    


if __name__ == "__main__":
    #freeze_support()
    #set_start_method("spawn")

    print(test('Publish'))