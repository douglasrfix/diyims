def test():
    """ """
    from rich import print
    from diyims.requests_utils import execute_request

    ipfs_header_CID = "bafkreidufrs6om7p3gyombkzv7s6gy3vlt45xt6sighqwt4v2jax4uvboq"
    ipfs_path = "/ipfs/" + ipfs_header_CID
    param = {"arg": ipfs_path}
    response, status_code, response_dict = execute_request(
        url_key="cat", param=param, timeout=(3.05, 100)
    )
    print(response_dict)


if __name__ == "__main__":
    test()
