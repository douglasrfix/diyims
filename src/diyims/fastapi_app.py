from fastapi import FastAPI, Request
from starlette.config import Config

# from starlette.requests import Request
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import HTMLResponse, RedirectResponse
from authlib.integrations.starlette_client import OAuth, OAuthError
from sqlalchemy.exc import NoResultFound
from datetime import datetime, timezone


# from datetime import datetime
# from fastapi import FastAPI, Request, status
from rich import print
# from typing import Annotated

# from sqlmodel import Field, Session, SQLModel, create_engine, select
from sqlmodel import Session, create_engine, select, col, delete
from diyims.path_utils import get_path_dict

# from fastapi.responses import HTMLResponse, RedirectResponse
# from starlette.status import HTTP_307_TEMPORARY_REDIRECT
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from diyims.sqlmodels import (
    Header_Chain_Status,
    Header_Table,
    Peer_Address,
    Peer_Table,
    Want_List_Table,
    Log,
    Directory,
    YT_Subscription,
    Repository,
)
from importlib.resources import files

# import os
# from rich import print
from pathlib import Path
# from sqlmodel import Session, delete, create_engine, SQLModel
# from diyims.sqlmodels import Directory
# from diyims.path_utils import get_path_dict
# from sqlmodel import delete

menu = {}
menu["root"] = "Home"
menu["html_log_list"] = "Log"
menu["html_peer_list"] = "Peer List"
menu["html_address_list"] = "Address list"
menu["html_want_list"] = "Want List"
menu["html_header_list"] = "Header List"
menu["html_header_status_list"] = "Header Status List"
menu["html_directory_list"] = "Directory List"
menu["html_subscription_list"] = "Subscription List"
menu["login"] = "Google Login"
menu["yt_apis"] = "YT APIs"


roaming = "Roaming"
# roaming = os.environ["DIYIMS_ROAMING"]
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
myapp.add_middleware(SessionMiddleware, secret_key="!secret")

config = Config("yt.env")
oauth = OAuth(config)

CONF_URL = "https://accounts.google.com/.well-known/openid-configuration"
oauth.register(
    name="google",
    server_metadata_url=CONF_URL,
    client_kwargs={
        "scope": "openid email profile https://www.googleapis.com/auth/youtube.readonly"
    },
)

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
    menu_translate = menu.copy()
    del menu_translate["html_address_list"]

    statement = select(Peer_Address).order_by(col(Peer_Address.insert_DTS).desc())
    with Session(engine) as session:
        address_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="address_list.html",
        context={
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Address List ",
            "address_list": address_list,
        },
    )


@myapp.get("/html/address_detail/", response_class=HTMLResponse)
async def html_address_detail(request: Request):
    menu_translate = menu.copy()

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
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Address Detail",
            "address": address,
            "address_string": address_string,
        },
    )


# need platform id file system?
@myapp.get("/html/directory_list/", response_class=HTMLResponse)
async def html_directory_list(request: Request):
    menu_translate = menu.copy()
    del menu_translate["html_directory_list"]

    path_dict = get_path_dict()
    sqlite_file_name = path_dict["db_file"]
    sqlite_url = f"sqlite:///{sqlite_file_name}"
    connect_args = {"check_same_thread": False}
    engine = create_engine(sqlite_url, echo=False, connect_args=connect_args)

    with Session(engine) as session:
        statement = delete(Directory)
        session.exec(statement)
        session.commit()

    cwd = Path("C:/Users/dougl/Documents")
    with Session(engine) as session:
        for root_dir, dirs_dir, filenames in Path(cwd).walk(on_error=print):
            if "__pycache__" in dirs_dir:
                dirs_dir.remove("__pycache__")
            if ".venv" in dirs_dir:
                dirs_dir.remove(".venv")
            if ".venv-11" in dirs_dir:
                dirs_dir.remove(".venv-11")
            if ".git" in dirs_dir:
                dirs_dir.remove(".git")

            new_entry = Directory(root=str(root_dir), file="")
            session.add(new_entry)
            for filename in filenames:
                new_entry = Directory(root=str(root_dir), file=str(filename))
                session.add(new_entry)
        session.commit()
    statement = select(Directory).order_by(Directory.root, Directory.file)
    with Session(engine) as session:
        directory_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="directory_list.html",
        context={
            "roaming": cwd,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Directory List",
            "directory_list": directory_list,
        },
    )


@myapp.get("/html/directory_detail/", response_class=HTMLResponse)
async def html_directory_detail(request: Request):
    menu_translate = menu.copy()

    root = request.query_params.get("root")
    file = request.query_params.get("file")
    p = Path(str(root), str(file))
    size = p.stat().st_size
    atime = p.stat().st_atime
    mtime = p.stat().st_mtime
    birthtime = p.stat().st_birthtime

    return templates.TemplateResponse(
        request=request,
        name="directory_detail.html",
        context={
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "File Detail",
            "root": root,
            "file": file,
            "size": size,
            "atime": atime,
            "mtime": mtime,
            "birthtime": birthtime,
        },
    )


@myapp.get("/html/header_list/", response_class=HTMLResponse)
async def html_header_list(request: Request):
    menu_translate = menu.copy()
    del menu_translate["html_header_list"]

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
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Header List ",
            "header_list": header_list,
        },
    )


@myapp.get("/html/header_detail/", response_class=HTMLResponse)
async def html_header_detail(request: Request):
    menu_translate = menu.copy()

    header_CID = request.query_params.get("header_CID")
    statement = select(Header_Table).where(Header_Table.header_CID == header_CID)
    with Session(engine) as session:
        header_detail = session.exec(statement).first()

    return templates.TemplateResponse(
        request=request,
        name="header_detail.html",
        context={
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Header Detail",
            "header_detail": header_detail,
        },
    )


@myapp.get("/html/header_status_list/", response_class=HTMLResponse)
async def html_header_status_list(request: Request):
    menu_translate = menu.copy()
    del menu_translate["html_header_status_list"]

    statement = select(Header_Chain_Status).order_by(
        col(Header_Chain_Status.insert_DTS).asc()
    )
    with Session(engine) as session:
        header_status_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="header_status_list.html",
        context={
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Header Status List ",
            "header_status_list": header_status_list,
        },
    )


@myapp.get("/html/header_status_detail/", response_class=HTMLResponse)
async def html_header_status_detail(request: Request):
    menu_translate = menu.copy()

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
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Header Status Detail",
            "header_status_detail": header_status_detail,
        },
    )


@myapp.get("/html/log_list/", response_class=HTMLResponse)
async def html_log_list(request: Request):
    menu_translate = menu.copy()
    del menu_translate["html_log_list"]

    # peer_ID = request.query_params.get("peer_ID")
    statement = select(Log).order_by(col(Log.DTS).asc())
    with Session(engine) as session:
        log_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="log_list.html",
        context={
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Log Entries",
            "log_list": log_list,
        },
    )


@myapp.get("/html/log_detail/", response_class=HTMLResponse)
async def html_log_detail(request: Request):
    menu_translate = menu.copy()

    DTS = request.query_params.get("DTS")
    pid = request.query_params.get("pid")
    statement = select(Log).where(Log.DTS == DTS, Log.pid == pid)
    with Session(engine) as session:
        log_entry = session.exec(statement).first()

    return templates.TemplateResponse(
        request=request,
        name="log_detail.html",
        context={
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Log Entry Detail",
            "log_entry": log_entry,
        },
    )


@myapp.get("/html/peer_address_list/", response_class=HTMLResponse)
async def html_peer_address_list(request: Request):
    menu_translate = menu.copy()

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
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Peer Addresses",
            "address_list": address_list,
            "peer_ID": peer_ID,
        },
    )


@myapp.get("/html/peer_list/", response_class=HTMLResponse)
async def html_peer_list(request: Request):
    menu_translate = menu.copy()
    del menu_translate["html_peer_list"]

    statement = select(Peer_Table).order_by(col(Peer_Table.local_update_DTS).desc())
    with Session(engine) as session:
        peer_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="peer_list.html",
        context={
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Peer List",
            "peer_list": peer_list,
        },
    )


@myapp.get("/html/peer_detail/", response_class=HTMLResponse)
async def html_peer_detail(request: Request):
    menu_translate = menu.copy()

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
            "roaming": roaming,
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


@myapp.get("/html/subscription_list/", response_class=HTMLResponse)
async def html_subscription_list(request: Request):
    menu_translate = menu.copy()
    del menu_translate["html_subscription_list"]

    statement = select(YT_Subscription).order_by(YT_Subscription.snippet_title)
    with Session(engine) as session:
        subscription_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="subscription_list.html",
        context={
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Subscription List",
            "subscription_list": subscription_list,
        },
    )


@myapp.get("/html/subscription_detail/", response_class=HTMLResponse)
async def html_subscription_detail(request: Request):
    menu_translate = menu.copy()

    id = request.query_params.get("id")
    statement_1 = select(YT_Subscription).where(YT_Subscription.id == id)

    with Session(engine) as session:
        subscription = session.exec(statement_1).one()

    return templates.TemplateResponse(
        request=request,
        name="subscription_detail.html",
        context={
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Subscription Detail",
            "id": subscription.id,
            "snippet_publishedAt": subscription.snippet_publishedAt,
            "snippet_title": subscription.snippet_title,
            "snippet_description": subscription.snippet_description,
            "snippet_resourceId_channelId": subscription.snippet_resourceId_channelId,
            "contentDetails_newItemCount": subscription.contentDetails_newItemCount,
        },
    )


@myapp.get("/html/want_list/", response_class=HTMLResponse)
async def html_want_list(request: Request):
    menu_translate = menu.copy()
    del menu_translate["html_want_list"]

    statement = select(Want_List_Table)
    with Session(engine) as session:
        want_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="want_list.html",
        context={
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Want Item List",
            "want_list": want_list,
        },
    )


@myapp.get("/html/want_detail/", response_class=HTMLResponse)
async def html_want_detail(request: Request):
    menu_translate = menu.copy()

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
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "title": "Want Item Detail",
            "want_item": want_item,
        },
    )


@myapp.get("/html/peer_want_list/", response_class=HTMLResponse)
async def html_peer_want_list(request: Request):
    menu_translate = menu.copy()

    peer_ID = request.query_params.get("peer_ID")
    statement = select(Want_List_Table).where(Want_List_Table.peer_ID == peer_ID)
    with Session(engine) as session:
        want_list = session.exec(statement).all()

    return templates.TemplateResponse(
        request=request,
        name="want_list.html",
        context={
            "roaming": roaming,
            "mode": mode,
            "menu_translate": menu_translate,
            "peer_ID": peer_ID,
            "title": "Want Item List",
            "want_list": want_list,
        },
    )


@myapp.get("/login")
async def login(request: Request):
    # skip if token is present and refresh is not required. Refresh as required
    with Session(engine) as session:
        statement = select(Repository)
        try:
            session.exec(statement).one()
            # return RedirectResponse(url='/') # return to home page
            pass
        except NoResultFound:
            pass

    # client_id is in config
    # redirect_uri is derived below
    # response_type = 'code' for web apps, specified in  function kwargs
    # scope specified in register client_kwargs
    # access_type = offline, specified in function kwargs
    # state  handled by client integration
    # include_granted_scopes = true, specified in function kwargs
    # enable_granular_consent  default = true
    # login_init unspecified
    # prompt first time only if not specified
    redirect_uri = request.url_for("auth")  # prepare redirect url to be sent to google

    return await oauth.google.authorize_redirect(
        request,
        redirect_uri,
        response_type="code",
        access_type="offline",
        include_granted_scopes="true",
        prompt="",
    )  # send request to google


@myapp.get("/auth")  # process google response
async def auth(request: Request):
    try:
        token = await oauth.google.authorize_access_token(request)
        print(token)
        authorization_time = datetime.now(timezone.utc).timestamp()
        refresh_time = int(authorization_time) + int(token["expires_in"])

        with Session(engine) as session:
            try:
                refresh_token = token["refresh_token"]
            except KeyError:
                refresh_token = ""

            statement = select(Repository)
            try:
                session.exec(statement).one()
            except NoResultFound:
                token1 = str(token).replace("True", "true")
                new_token = Repository(
                    token=str(token1).replace("'", '"'),
                    access_token=str(token["access_token"]),
                    expires_in=str(token["expires_in"]),
                    refresh_token=str(refresh_token),
                    scope=str(token["scope"]),
                    token_type=str(token["token_type"]),
                    id_token=str(token["id_token"]),
                    expires_at=str(token["expires_at"]),
                    authorization_time=str(authorization_time),
                    refresh_time=str(refresh_time),
                )

            session.add(new_token)
            session.commit()

    except OAuthError as error:
        return HTMLResponse(f"<h1>{error.error}</h1>")

    return RedirectResponse(url="/")  # return to home page


@myapp.get("/yt_apis", response_class=HTMLResponse)
async def yt_apis(request: Request):
    menu_translate = menu.copy()

    context = {
        "roaming": roaming,
        "mode": mode,
        "menu_translate": menu_translate,
        "title": "DIYIMS",
    }
    with Session(engine) as session:
        statement = select(Repository)
        try:
            results = session.exec(statement)
            token = results.one()

        except NoResultFound:
            pass
    access_token = dict(token)
    print(access_token)

    url = "https://www.googleapis.com/youtube/v3/subscriptions?part=snippet&mine=true"
    result = await oauth.google.get(url, token=access_token)
    print(result)
    data = result.json()
    print("New data received:", data)

    return templates.TemplateResponse(
        request=request, name="base.html", context=context
    )


@myapp.get("/", response_class=HTMLResponse)
async def root(request: Request):
    menu_translate = menu.copy()
    with Session(engine) as session:
        statement = select(Repository)
        try:
            session.exec(statement).one()
            del menu_translate["login"]
        except NoResultFound:
            del menu_translate["yt_apis"]

    context = {
        "roaming": roaming,
        "mode": mode,
        "menu_translate": menu_translate,
        "title": "DIYIMS",
    }
    return templates.TemplateResponse(
        request=request, name="base.html", context=context
    )
