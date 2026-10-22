import json

#import shutil
from pathlib import Path

# from diyims.path_utils import get_path_dict
from sqlmodel import Session, create_engine

from diyims.config_utils import get_publish_config_dict
from diyims.general_utils import get_DTS, set_self
from diyims.header_utils import ipfs_header_add
from diyims.ipfs_utils import get_url_dict
from diyims.logger_utils import add_log
from diyims.path_utils import get_path_dict, get_unique_file
from diyims.requests_utils import execute_request
from diyims.sqlmodels import (
    Object_Meta_Data,
)


def cli_file_add(call_stack, src_file_path):
    call_stack = call_stack + ":cli_file_add"
    
    config_dict = get_publish_config_dict()
    path_dict = get_path_dict()

    
        
    work_path = Path(src_file_path)
    file_name = work_path.name # includes stem and suffix
    file_type = work_path.suffix


    if file_type == ".whl":
        src_path = (
            Path.cwd().joinpath("dist").joinpath(file_name)
        )
    else:
        src_path = (
                    Path.cwd().joinpath(file_name) #TODO: this is error prone
                )


        
    #out_file_path = Path(path_dict["generic_path"]).joinpath(file_name)  

    url_dict = get_url_dict()
    
    self = set_self().self
    
    #shutil.copyfile(src_file_path, out_file_path)
        
    file_CID = file_add(call_stack, src_path)
    

    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
    DTS = get_DTS()
    meta_dict = {}
    meta_dict["version"] = "1"
    meta_dict["object_CID"] = file_CID

    meta_dict["object_type"] = "generic_meta"
    meta_dict["insert_DTS"] = DTS
    meta_dict["peer_ID"] = self
    meta_dict["meta_data"] = {"file_name": file_name}

    proto_path = path_dict["peer_path"]
    proto_file = path_dict["peer_file"]
    proto_file_path = get_unique_file(proto_path, proto_file)

    #This is meta data which need not be retained
    param = {
        "cid-version": 1,
        "only-hash": "false",
        "pin": "false",
    }
    with open(proto_file_path, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(meta_dict, write_file, indent=4)

    with open(proto_file_path, "rb") as f:
        add_file = {"file": f}
        _response, _status_code, response_dict = execute_request(
            url_key="add",
            url_dict=url_dict,
            config_dict=config_dict, # Correct dictionary ?
            file=add_file,
            param=param,
            call_stack=call_stack,
            http_500_ignore=False,
        )
    try:
        Path(proto_file_path).unlink()
    except FileNotFoundError:
        pass
        
    meta_CID = response_dict["Hash"]  # new peer row cid
   

    # meta_dict = jsonable_encoder(telemetry) # TODO: necessary?
    if file_type != ".whl":
        new_meta = Object_Meta_Data(
            version=meta_dict["version"],
            object_CID=meta_dict["object_CID"],
            object_type=meta_dict["object_type"],
            peer_ID=meta_dict["peer_ID"],
            insert_DTS=meta_dict["insert_DTS"],
            meta_data=meta_dict["meta_data"],
            meta_CID=meta_CID,  # prior meta cid is ignored for now
        )
        # print(meta_dict["object_CID"])
        # return
        with Session(engine) as session:
            session.add(new_meta)
            session.commit()

    pin_file(call_stack, file_CID)
    pin_meta(call_stack, meta_CID)

    peer_ID = self
    object_CID = meta_CID
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
        config_dict,# ??
        mode,
        processing_status,
        #processing_status_DTS,
        publish_wakeup_suppressed,
    )


def file_add(call_stack, file_name): #TODO: #19 name is misleading
    call_stack = call_stack + ":file_add"  # step 2
    
    path_dict = get_path_dict()
    
    src_path = Path(path_dict["generic_path"]).joinpath(file_name)
    url_dict = get_url_dict()
    config_dict = get_publish_config_dict()

    param = {
        "cid-version": 1,
        "only-hash": "false",
        "pin": "false",
    }
    
    # This file is not unlinked
    with open(src_path, "rb") as f:
        # print("file opened")
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
        #print("add processed")

    if status_code == 200:
        # This is a original source file and is not unlinked
        pass 
    else:
        #print(status_code)
        add_log(
            process=call_stack,
            peer_type="Error",
            msg="Add Object_CID add failed Panic.",
        )

        return status_code

    
    object_CID = response_dict["Hash"]  # new peer row cid
    
    return object_CID


def pin_file(call_stack, object_CID):
    # print(object_CID)
    call_stack = call_stack + ":pin_file"
    config_dict = get_publish_config_dict()
    url_dict = get_url_dict()

    pin_add_params = {"arg": object_CID, "pin-name": "whl_add"}

    _response, _status_code, _response_dict = execute_request(
        url_key="pin_add",
        # logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        # file=dag_import_files,
        param=pin_add_params,
        call_stack=call_stack,
    )
    


def pin_meta(call_stack, object_CID):
    # print(object_CID)
    call_stack = call_stack + ":pin_meta"
    config_dict = get_publish_config_dict()
    url_dict = get_url_dict()

    pin_add_params = {"arg": object_CID, "pin-name": "meta_add"}

    _response, _status_code, _response_dict = execute_request(
        url_key="pin_add",
        # logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        # file=dag_import_files,
        param=pin_add_params,
        call_stack=call_stack,
    )
    




if __name__ == "__main__":
    cli_file_add("cmd", "c:\\Users\\dougl\\diyims\\diyims\\README.md")
