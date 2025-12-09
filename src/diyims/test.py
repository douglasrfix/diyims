""" """

from rich import print
from diyims.path_utils import get_path_dict

# from fastapi.encoders import jsonable_encoder
# from fastapi.responses import JSONResponse
from sqlmodel import create_engine, Session, select
from diyims.sqlmodels import Peer_Table


path_dict = get_path_dict()
sqlite_file_name = path_dict["db_file"]
sqlite_url = f"sqlite:///{sqlite_file_name}"
connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)


statement1 = (
    select(Peer_Table)
    #    .where(Peer_Table.processing_status == "NCP")
    .where(Peer_Table.peer_type == "LP")
)

statement2 = (
    select(Peer_Table)
    # .where((Peer_Table.processing_status == "WLR"))
    # .where(Peer_Table.peer_type == peer_type)
)
with Session(engine) as session:
    results = session.exec(statement2)
    peer_table_rows = results.all()  # produce a list of peers to process


print(peer_table_rows)

# with Session(engine) as session:
#    results = session.exec(statement1).one()

# peer_table_row_dict = dict(results)
# print(peer_table_row_dict)
# peer_table_row = Peer_Table(**peer_table_row_dict)
# print(peer_table_row)
# employee_data = {'firstname':'John','lastname':'Smith'}
# employee = Employee(**employee_data)
# db.session.add(employee)
# db.session.commit()
# status_code, content = JSONResponse(status_code=200, content=json_compatible)
# print(status_code, content)
