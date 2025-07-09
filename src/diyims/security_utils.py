def sign_file(sign_dict, logger, config_dict):
    from diyims.requests_utils import execute_request
    from diyims.ipfs_utils import get_url_dict

    url_dict = get_url_dict()

    sign_params = {}

    f = open(sign_dict["file_to_sign"], "rb")
    sign_files = {"file": f}
    response, status_code, response_dict = execute_request(
        url_key="sign",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        file=sign_files,
        param=sign_params,
    )
    f.close()

    id = response_dict["Key"]["Id"]
    signature = response_dict["Signature"]

    return id, signature


def verify_file(verify_dict, logger, config_dict):
    from diyims.requests_utils import execute_request
    from diyims.ipfs_utils import get_url_dict

    url_dict = get_url_dict()

    verify_params = {"key": verify_dict["id"], "signature": verify_dict["signature"]}

    f = open(verify_dict["signed_file"], "rb")
    verify_files = {"file": f}
    response, status_code, response_dict = execute_request(
        url_key="verify",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        file=verify_files,
        param=verify_params,
    )
    f.close()

    signature_valid = response_dict["SignatureValid"]

    return signature_valid


def verify_peer_row_from_cid(peer_row_CID, logger, config_dict):
    from diyims.ipfs_utils import unpack_peer_row_from_cid
    from diyims.path_utils import get_path_dict
    import json

    path_dict = get_path_dict()

    peer_row_dict = unpack_peer_row_from_cid(peer_row_CID, config_dict)

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

    signature_verified = verify_file(verify_dict, logger, config_dict)

    return signature_verified, peer_row_dict
