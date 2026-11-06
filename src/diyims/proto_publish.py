import json
import os
from pathlib import Path

from fastapi.encoders import jsonable_encoder
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from diyims.config_utils import get_config_dict
from diyims.general_utils import get_DTS, set_self
from diyims.header_utils import ipfs_header_add
from diyims.ipfs_utils import get_url_dict
from diyims.logger_utils import add_log
from diyims.path_utils import get_path_dict, get_unique_file
from diyims.requests_utils import execute_request
from diyims.sqlmodels import (
    Config,
    Object_Meta_Data,
)


def publish_config_table(call_stack: str):
    call_stack = call_stack + ":publish_config_table"

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
    with Session(engine) as session:
                result_list = session.execute(Config.__table__.select()).mappings().all()
                jsonable_result = jsonable_encoder(result_list)
                data = {}
                data['config_file'] = jsonable_result    
    
    meta_add(call_stack,
             pin_name = 'system_file_add',
             reference_object_type = 'system_file',
             data_dictionary = data,
             target_path = 'update_path',
             file_name = 'config.tbl',
             file_suffix = '.tbl',
             unlink = True
             )    


def cli_file_add(call_stack, cli_file_path):
    call_stack = call_stack + ":cli_file_add"

    work_path = Path(cli_file_path)
    file_name = work_path.name # includes stem and suffix
    file_suffix = work_path.suffix

    if file_suffix == ".whl":
        source_path = (
            Path.cwd().joinpath("dist").joinpath(file_name)
        )
        pin_name = 'system_file_add'
        reference_object_type = 'system_file'
        target_path = 'update_path'
    else:
        source_path = (
            Path.cwd().joinpath(file_name)
        )
        pin_name = 'generic_file_add'
        reference_object_type = 'generic_file'
        target_path = 'generic_path'

    meta_add(call_stack,
             pin_name = pin_name,
             reference_object_type = reference_object_type,
             file_name = file_name,
             source_path = source_path,
             target_path = target_path,
             unlink = False)    


def meta_add(call_stack: str, **kwargs):
    
    url_dict = get_url_dict()
    self = set_self().self
    config_dict = get_config_dict('File_add')
    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    

    try:
        _test = kwargs["data_dictionary"] # if a data dictionary is provided then use it
        try:
            proto_path = path_dict["temp_path"]
            proto_file = path_dict["temp_file"]
            source_path = get_unique_file(proto_path, proto_file)
            with open(source_path, "w", encoding="utf-8", newline="\n") as write_file: 
                json.dump(kwargs['data_dictionary'], write_file, indent=4)
        except KeyError:
            pass
            
    except KeyError:
        source_path = kwargs['source_path'] # if a data dictionary is not provided use the source path from kwargs 

        
    reference_object_CID = file_add(call_stack,
                                    source_path = source_path,
                                    pin_name = kwargs['pin_name'],
                                    unlink = kwargs['unlink'],
                                    )
    
    DTS = get_DTS()
    meta_dict = {}
    meta_dict["version"] = "1"
    meta_dict["reference_object_CID"] = reference_object_CID
    meta_dict["reference_object_type"] = kwargs['reference_object_type']
    meta_dict["peer_ID"] = self
    meta_dict["insert_DTS"] = DTS
    # meta_dict["object_CID"]  is not present in dictionary   
    meta_dict["object_type"] = 'meta'   
    meta_dict["reference_meta_data"]= {'file_name': kwargs['file_name'], 'target_path': kwargs['target_path']}
    meta_dict["reference_tags"] = ''

    proto_path = path_dict["temp_path"]
    proto_file = path_dict["temp_file"]
    meta_file_path = get_unique_file(proto_path, proto_file)

    param = {
        "cid-version": 1,
        "only-hash": "false",
        "pin": "true",
        'pin-name': kwargs['pin_name'],
    }
    with open(meta_file_path, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(meta_dict, write_file, indent=4)

    with open(meta_file_path, "rb") as f:
        add_file = {"file": f}
        _response, _status_code, response_dict = execute_request(
            url_key="add",
            url_dict=url_dict,
            config_dict=config_dict,
            file=add_file,
            param=param,
            call_stack=call_stack,
            http_500_ignore=False,
        )
    
    try:
        Path(meta_file_path).unlink()
    except FileNotFoundError:
        pass
        
    meta_dict["object_CID"] = response_dict["Hash"]

    
    new_meta = Object_Meta_Data(
        version=meta_dict["version"],
        reference_object_CID=meta_dict["reference_object_CID"],
        reference_object_type=meta_dict["reference_object_type"],
        peer_ID=meta_dict["peer_ID"],
        insert_DTS=DTS,
        object_CID=meta_dict["object_CID"],  
        object_type=meta_dict["object_type"],
        # prior meta cid is ignored for now
        reference_meta_data=meta_dict["reference_meta_data"],
        reference_tags='',    
    )
    
    with Session(engine) as session:
        session.add(new_meta)
        session.commit()

    peer_ID = self
    object_CID = meta_dict["object_CID"]
    object_type = meta_dict["object_type"]

    mode = object_type
    processing_status = "publish-requested"
    publish_wakeup_suppressed = False
    #processing_status_DTS = DTS

    _status_code, _header_CID = ipfs_header_add(
        call_stack,
        DTS,
        object_CID,
        object_type,
        peer_ID,
        config_dict,
        mode, #TODO:  issue 19 fix name
        processing_status,
        #processing_status_DTS,
        publish_wakeup_suppressed,
    )


def file_add(call_stack, **kwargs):
    
    call_stack = call_stack + ":file_add"
    
    url_dict = get_url_dict()
    config_dict = get_config_dict('Publish')

    param = {
        "cid-version": 1,
        "only-hash": "false",
        "pin": "true",
        'pin_name': kwargs['pin_name'],
    }
    
    with open(kwargs['source_path'], "rb") as f:
        add_file = {"file": f}
        _response, status_code, response_dict = execute_request(
            url_key="add",
            url_dict=url_dict,
            config_dict=config_dict,
            file=add_file,
            param=param,
            call_stack=call_stack,
            http_500_ignore=False,
        )

    if kwargs['unlink']:    

        try:
            Path(kwargs['source_path']).unlink()
        except FileNotFoundError:
            pass


    if status_code != 200:
        add_log( 
            process=call_stack,
            peer_type="Error",
            msg="Add Object_CID add failed Panic.",
        )

        return status_code

    
    object_CID = response_dict["Hash"]
    
    return object_CID # TODO: clean up inconsistent returns 

if __name__ == "__main__":
    os.environ["QUEUES_ENABLED"] = "0"
    #cli_file_add("cmd", "README.md")
    publish_config_table('cmd')
