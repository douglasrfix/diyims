import json

from diyims.logger_utils import add_log
from diyims.requests_utils import execute_request
from diyims.path_utils import get_path_dict
from diyims.ipfs_utils import unpack_object_from_cid


def sign_file(
    call_stack,
    sign_dict,
):
    call_stack = call_stack + ":sign_file"
    sign_params = {}

    f = open(sign_dict["file_to_sign"], "rb")
    sign_files = {"file": f}
    response, status_code, response_dict = execute_request(
        url_key="sign",
        file=sign_files,
        param=sign_params,
        call_stack=call_stack,
        http_500_ignore=False,
    )
    f.close()
    if status_code == 200:
        id = response_dict["Key"]["Id"]
        signature = response_dict["Signature"]
    else:
        add_log(
            process=call_stack,
            peer_type="Error",
            msg="Sign File Panic.",
        )
        id = ""
        signature = ""

    return status_code, id, signature


def verify_file(
    call_stack,
    verify_dict,
):
    call_stack = call_stack + ":verify_file"
    verify_params = {"key": verify_dict["id"], "signature": verify_dict["signature"]}

    f = open(verify_dict["signed_file"], "rb")
    verify_files = {"file": f}
    response, status_code, response_dict = execute_request(
        url_key="verify",
        file=verify_files,
        param=verify_params,
        call_stack=call_stack,
        http_500_ignore=False,
    )
    f.close()

    if status_code == 200:
        signature_valid = response_dict["SignatureValid"]
    else:
        add_log(
            process=call_stack,
            peer_type="Error",
            msg="Verify File Panic.",
        )
        signature_valid = 0

    return status_code, signature_valid


def verify_peer_row_from_cid(
    call_stack,
    peer_row_CID,
):
    path_dict = get_path_dict()
    call_stack = call_stack + "verify_peer_row_from_cid"
    status_code, peer_row_dict = unpack_object_from_cid(
        call_stack,
        peer_row_CID,
    )

    if status_code != 200:
        add_log(
            process=call_stack,
            peer_type="Error",
            msg="Verify Peer Row Panic.",
        )
        signature_verified = 0
        peer_row_dict = {}
        return status_code, signature_verified, peer_row_dict

    signing_dict = {}
    signing_dict["peer_ID"] = peer_row_dict["peer_ID"]

    file_to_verify = path_dict[
        "sign_file"
    ]  # NOTE: generate unique name via queue server?
    with open(file_to_verify, "w", encoding="utf-8", newline="\n") as write_file:
        json.dump(signing_dict, write_file, indent=4)

    verify_dict = {}
    verify_dict["signed_file"] = file_to_verify
    verify_dict["id"] = peer_row_dict["id"]
    verify_dict["signature"] = peer_row_dict["signature"]

    status_code, signature_verified = verify_file(
        call_stack,
        verify_dict,
    )
    if status_code != 200:
        add_log(
            process=call_stack,
            peer_type="Error",
            msg="Verify Peer Row Panic.",
        )
        signature_verified = 0
        peer_row_dict = {}
    return status_code, signature_verified, peer_row_dict
