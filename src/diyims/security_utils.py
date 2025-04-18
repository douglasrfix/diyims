def sign_file(sign_dict, logger, config_dict):
    from diyims.requests_utils import execute_request
    from diyims.ipfs_utils import get_url_dict

    url_dict = get_url_dict()
    sign_files = {"file": open(sign_dict["file_to_sign"], "rb")}
    sign_params = {}

    response, status_code, response_dict = execute_request(
        url_key="sign",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        file=sign_files,
        param=sign_params,
    )

    id = response_dict["Key"]["Id"]
    signature = response_dict["Signature"]

    return id, signature


def verify_file(verify_dict, logger, config_dict):
    from diyims.requests_utils import execute_request
    from diyims.ipfs_utils import get_url_dict

    url_dict = get_url_dict()

    verify_files = {"file": open(verify_dict["signed_file"], "rb")}
    verify_params = {"key": verify_dict["id"], "signature": verify_dict["signature"]}

    response, status_code, response_dict = execute_request(
        url_key="verify",
        logger=logger,
        url_dict=url_dict,
        config_dict=config_dict,
        file=verify_files,
        param=verify_params,
    )

    signature_valid = response_dict["SignatureValid"]

    return signature_valid
