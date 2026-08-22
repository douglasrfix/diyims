# from rich import print
from diyims.requests_utils import execute_request
from diyims.path_utils import get_path_dict


def test(call_stack):
    # from diyims.path_utils import get_path_dict
    ipfs_sourced_header_CID = "QmYYqUt8DfpHWjjJ9hVGFfnCVd3zrbHwgTom3jmZBVmxA8"
    path_dictionary = get_path_dict()
    ipfs_path = "/ipfs/" + ipfs_sourced_header_CID
    param = {"arg": ipfs_path}
    outfile = str(path_dictionary["update_path"]) + "\\testfile.whl"
    # print(outfile)
    response, status_code, response_dictionary = execute_request(
        url_key="cat",
        param=param,
        timeout=(3.05, 122),  # avoid timeouts
        call_stack=call_stack,
        http_500_ignore=False,
        stream=True,
        outfile=outfile,  # TODO: user interface to support entry of the file name with a predefined path
    )
    # TODO: Support publishing as well as retrieval into standard library

    # print(status_code)

    return


if __name__ == "__main__":
    test("cmd")

    # added QmYYqUt8DfpHWjjJ9hVGFfnCVd3zrbHwgTom3jmZBVmxA8 diyims-0.0.0a178-py3-none-any.whl
