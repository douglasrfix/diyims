# from datetime import datetime
from fastapi import FastAPI, Request
# from rich import print
# from typing import Annotated

# from sqlmodel import Field, Session, SQLModel, create_engine, select
from sqlmodel import Session, create_engine, select, col
from diyims.path_utils import get_path_dict
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from diyims.sqlmodels import (
    Header_Chain_Status,
    Header_Table,
    Peer_Address,
    Peer_Table,
    Want_List_Table,
)
from importlib.resources import files


mode = {}
mode["dark"] = 1

path_dict = get_path_dict()
sqlite_file_name = path_dict["db_file"]
sqlite_url = f"sqlite:///{sqlite_file_name}"

connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

static_path = files("diyims.static")
font_path = files("diyims.fonts")
template_path = files("diyims.templates")

myapp = FastAPI()

myapp.mount("/static", StaticFiles(directory=static_path), name="static")
myapp.mount("/fonts", StaticFiles(directory=font_path), name="fonts")
templates = Jinja2Templates(directory=template_path)


@myapp.get("/items/{id}", response_class=HTMLResponse)
async def read_item(request: Request, id: str):
    return templates.TemplateResponse(
        request=request, name="item.html", context={"id": id}
    )


@myapp.post("/api/addresses/", response_model=Peer_Address)
def create_peer_address(peer_address: Peer_Address):
    with Session(engine) as session:
        session.add(peer_address)
        session.commit()
        session.refresh(peer_address)
        return peer_address


@myapp.get("/api/addresses/", response_model=list[Peer_Address])
def read_peer_addresses():
    with Session(engine) as session:
        peer_addresses = session.exec(select(Peer_Address)).all()
        return peer_addresses


@myapp.get("/api/want_list/", response_model=list[Want_List_Table])
def read_want_list_table():
    with Session(engine) as session:
        want_lists = session.exec(select(Want_List_Table)).all()
        return want_lists


@myapp.get("/html/address_list/", response_class=HTMLResponse)
async def html_address_list(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    # menu_translate["html_address_list"] = "Address list"
    menu_translate["html_want_list"] = "Want List"
    menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"

    statement = select(Peer_Address).order_by(col(Peer_Address.insert_DTS).desc())
    with Session(engine) as session:
        address_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="address_list.html",
        context={
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Address List ",
            "address_list": address_list,
        },
    )


@myapp.get("/html/address_detail/", response_class=HTMLResponse)
async def html_address_detail(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    menu_translate["html_address_list"] = "Address List"
    menu_translate["html_want_list"] = "Want List"
    menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"

    address_string = request.query_params.get("address_string")
    statement = select(Peer_Address).where(
        Peer_Address.address_string == address_string
    )
    with Session(engine) as session:
        address = session.exec(statement).first()

    return templates.TemplateResponse(
        request=request,
        name="address_detail.html",
        context={
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Address Detail",
            "address": address,
            "address_string": address_string,
        },
    )


@myapp.get("/html/header_list/", response_class=HTMLResponse)
async def html_header_list(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    menu_translate["html_address_list"] = "Address List"
    menu_translate["html_want_list"] = "Want List"
    # menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"

    statement = (
        select(Header_Table)
        .order_by(col(Header_Table.peer_ID).asc())
        .order_by(col(Header_Table.insert_DTS).desc())
    )
    with Session(engine) as session:
        header_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="header_list.html",
        context={
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Header List ",
            "header_list": header_list,
        },
    )


@myapp.get("/html/header_detail/", response_class=HTMLResponse)
async def html_header_detail(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    menu_translate["html_address_list"] = "Address List"
    menu_translate["html_want_list"] = "Want List"
    menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"

    header_CID = request.query_params.get("header_CID")
    statement = select(Header_Table).where(Header_Table.header_CID == header_CID)
    with Session(engine) as session:
        header_detail = session.exec(statement).first()

    return templates.TemplateResponse(
        request=request,
        name="header_detail.html",
        context={
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Header Detail",
            "header_detail": header_detail,
        },
    )


@myapp.get("/html/header_status_list/", response_class=HTMLResponse)
async def html_header_status_list(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    menu_translate["html_address_list"] = "Address List"
    menu_translate["html_want_list"] = "Want List"
    menu_translate["html_header_list"] = "Header List"
    # menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"

    statement = select(Header_Chain_Status).order_by(
        col(Header_Chain_Status.insert_DTS).asc()
    )
    with Session(engine) as session:
        header_status_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="header_status_list.html",
        context={
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Header Status List ",
            "header_status_list": header_status_list,
        },
    )


@myapp.get("/html/header_status_detail/", response_class=HTMLResponse)
async def html_header_status_detail(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    menu_translate["html_address_list"] = "Address List"
    menu_translate["html_want_list"] = "Want List"
    menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"

    insert_DTS = str(request.query_params.get("insert_DTS"))
    peer_ID = request.query_params.get("peer_ID")
    missing_header_CID = request.query_params.get("missing_header_CID")
    statement = (
        select(Header_Chain_Status)
        .where(Header_Chain_Status.insert_DTS == insert_DTS)
        .where(Header_Chain_Status.peer_ID == peer_ID)
        .where(Header_Chain_Status.missing_header_CID == missing_header_CID)
    )
    with Session(engine) as session:
        header_status_detail = session.exec(statement).first()

    return templates.TemplateResponse(
        request=request,
        name="header_status_detail.html",
        context={
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Header Status Detail",
            "header_status_detail": header_status_detail,
        },
    )


@myapp.get("/html/peer_address_list/", response_class=HTMLResponse)
async def peer_addresses(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    menu_translate["html_address_list"] = "Address List"
    menu_translate["html_want_list"] = "Want List"
    menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"

    peer_ID = request.query_params.get("peer_ID")
    statement = (
        select(Peer_Address)
        .where(Peer_Address.peer_ID == peer_ID)
        .order_by(col(Peer_Address.insert_DTS).asc())
    )
    with Session(engine) as session:
        address_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="address_list.html",
        context={
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Peer Addresses",
            "address_list": address_list,
            "peer_ID": peer_ID,
        },
    )


@myapp.get("/html/peer_list/", response_class=HTMLResponse)
async def html_peer_list(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    # menu_translate["html_peer_list"] = "Peer List"
    menu_translate["html_address_list"] = "Address List"
    menu_translate["html_want_list"] = "Want List"
    menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"

    statement = select(Peer_Table).order_by(col(Peer_Table.local_update_DTS).desc())
    with Session(engine) as session:
        peer_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="peer_list.html",
        context={
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Peer List",
            "peer_list": peer_list,
        },
    )


@myapp.get("/html/peer_detail/", response_class=HTMLResponse)
async def html_peer_detail(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    menu_translate["html_address_list"] = "Address List"
    menu_translate["html_want_list"] = "Want List"
    menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"

    peer_ID = request.query_params.get("peer_ID")

    statement_1 = select(Peer_Table).where(Peer_Table.peer_ID == peer_ID)
    statement_2 = (
        select(Peer_Address)
        .where(Peer_Address.peer_ID == peer_ID)
        .order_by(col(Peer_Address.in_use).desc())
        .order_by(col(Peer_Address.address_global).desc())
        .order_by(col(Peer_Address.insert_DTS).desc())
    )
    statement_3 = (
        select(Want_List_Table)
        .where(Want_List_Table.peer_ID == peer_ID)
        .order_by(col(Want_List_Table.insert_DTS).desc())
    )
    statement_4 = (
        select(Header_Chain_Status)
        .where(Header_Chain_Status.peer_ID == peer_ID)
        .order_by(col(Header_Chain_Status.insert_DTS).asc())
    )
    statement_5 = (
        select(Header_Table)
        .where(Header_Table.peer_ID == peer_ID)
        .order_by(col(Header_Table.insert_DTS).asc())
    )
    with Session(engine) as session:
        peer = session.exec(statement_1).first()
        # TODO: condition on not LP
        address_list = session.exec(statement_2).all()
        want_list = session.exec(statement_3).all()
        header_status_list = session.exec(statement_4).all()
        header_list = session.exec(statement_5).all()

    return templates.TemplateResponse(
        request=request,
        name="peer_detail.html",
        context={
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Peer Detail",
            "peer": peer,
            "address_list": address_list,
            "want_list": want_list,
            "header_list": header_list,
            "header_status_list": header_status_list,
        },
    )


@myapp.get("/html/want_list/", response_class=HTMLResponse)
async def html_want_list(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    menu_translate["html_address_list"] = "Address List"
    # menu_translate["html_want_list"] = "Want List"
    menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"

    statement = select(Want_List_Table)
    with Session(engine) as session:
        want_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="want_list.html",
        context={
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Want Item List",
            "want_list": want_list,
        },
    )


@myapp.get("/html/want_detail/", response_class=HTMLResponse)
async def html_want_detail(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    menu_translate["address_list"] = "Address List"
    menu_translate["html_want_list"] = "Want List"
    menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"

    peer_ID = request.query_params.get("peer_ID")
    object_CID = request.query_params.get("object_CID")
    statement = select(Want_List_Table).where(
        Want_List_Table.peer_ID == peer_ID,
        Want_List_Table.object_CID == object_CID,
    )
    with Session(engine) as session:
        want_item = session.exec(statement).first()

    return templates.TemplateResponse(
        request=request,
        name="want_detail.html",
        context={
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Want Item Detail",
            "want_item": want_item,
        },
    )


@myapp.get("/html/peer_want_list/", response_class=HTMLResponse)
async def html_peer_want_list(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    menu_translate["address_list"] = "Address List"
    # menu_translate["html_want_list"] = "Want list"
    menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"

    peer_ID = request.query_params.get("peer_ID")
    statement = select(Want_List_Table).where(Want_List_Table.peer_ID == peer_ID)
    with Session(engine) as session:
        want_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="want_list.html",
        context={
            "mode": mode,
            "menu_translate": menu_translate,
            "peer_ID": peer_ID,
            "title": "Want Item List",
            "want_list": want_list,
        },
    )


@myapp.get("/user/", response_class=HTMLResponse)
async def user(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    menu_translate["html_address_list"] = "Address List"
    menu_translate["html_want_list"] = "Want List"
    menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"
    # with Session(engine) as session:
    #    addresses = session.exec(select(Peer_Address)).all()

    context = {
        "mode": mode,
        "menu_translate": menu_translate,
        "title": "DIYIMS",
    }
    return templates.TemplateResponse(
        request=request, name="user.html", context=context
    )


@myapp.get("/", response_class=HTMLResponse)
async def root(request: Request):
    menu_translate = {}
    menu_translate["root"] = "Home"
    menu_translate["html_peer_list"] = "Peer List"
    menu_translate["html_address_list"] = "Address List"
    menu_translate["html_want_list"] = "Want List"
    menu_translate["html_header_list"] = "Header List"
    menu_translate["html_header_status_list"] = "Header Status List"
    menu_translate["user"] = "User"
    # with Session(engine) as session:
    #    addresses = session.exec(select(Peer_Address)).all()

    context = {
        "mode": mode,
        "menu_translate": menu_translate,
        "title": "DIYIMS",
    }
    return templates.TemplateResponse(
        request=request, name="base.html", context=context
    )
