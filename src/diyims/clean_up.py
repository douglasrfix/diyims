#from datetime import UTC, datetime, timedelta
from datetime import datetime, timedelta

#from pathlib import Path
from sqlmodel import Session, create_engine, select

from diyims.config_utils import get_config_dict
from diyims.path_utils import get_path_dict

#from diyims.requests_utils import execute_request


def clean_up(call_stack, roaming):
    from diyims.sqlmodels import Log

    call_stack = call_stack + ":clean_up"
    config_dict = get_config_dict('Clean_Up')

    hours_to_delay = config_dict["hours_to_delay"]
    end_time = datetime.today() - timedelta(hours=int(hours_to_delay))  # noqa: DTZ002

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    #print(sqlite_file_name)
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)
    
    statement = select(Log).where(Log.DTS <= end_time.isoformat()) #TODO: #40 trim log function should be invoked at start up and this logic doesn't look right
    with Session(engine) as session:
        results = session.exec(statement).all()
        for log in results:
            session.delete(log)
        session.commit  # noqa: B018

    
    path = get_path_dict()
    folder_path = path["sign_path"]

    if not folder_path.is_dir(): # TODO: #38 put this in the sign function
        raise NotADirectoryError(f"{folder_path} is not a directory")
        
    for item in folder_path.iterdir():
        
        item.unlink()


