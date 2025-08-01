def main(roaming: str) -> None:
    from diyims.general_utils import exec_uvicorn

    exec_uvicorn(roaming)

    return


if __name__ == "__main__":
    main("Roaming")
