#from datetime import UTC, datetime, timedelta
from datetime import datetime, timedelta
from pathlib import Path

from sqlmodel import Session, create_engine, or_, select

from diyims.config_utils import get_clean_up_config_dict
from diyims.path_utils import get_path_dict
from diyims.requests_utils import execute_request
from diyims.sqlmodels import Beacon


def clean_up(call_stack, roaming):
    from diyims.sqlmodels import Clean_Up, Log

    call_stack = call_stack + ":clean_up"
    config_dict = get_clean_up_config_dict()

    hours_to_delay = config_dict["hours_to_delay"]
    end_time = datetime.today() - timedelta(hours=int(hours_to_delay))  # noqa: DTZ002

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    #print(sqlite_file_name)
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    # statement = select(Clean_Up)
    statement = select(Clean_Up).where(
        or_(Clean_Up.insert_DTS <= end_time.isoformat(), Clean_Up.status == "new")
    )
    with Session(engine) as session:
        results = session.exec(statement).all()

        for clean_up in results:
            want_item_file = clean_up.want_item_file
            beacon_CID = clean_up.beacon_CID

            try:
                Path(want_item_file).unlink()
            except FileNotFoundError:
                pass

            param = {
                "arg": beacon_CID,
            }

            _response, _status_code, _response_dict = execute_request(
                url_key="pin_remove",
                param=param,
                call_stack=call_stack,
            )
            # TODO: 700
            session.delete(clean_up)
            session.commit()

    statement = select(Log).where(Log.DTS <= end_time.isoformat())
    with Session(engine) as session:
        results = session.exec(statement).all()
        for log in results:
            session.delete(log)
        session.commit  # noqa: B018

    statement = select(Beacon).where(Beacon.status == "new")
    with Session(engine) as session:
        results = session.exec(statement)
        beacon_list = results.all()

    for beacon in beacon_list:     
        with Session(engine) as session:
            session.delete(beacon)
            session.commit()
    