from diyims.general_utils import run_fastapi


def main(roaming: str) -> None:
    run_fastapi(roaming)
    return


if __name__ == "__main__":
    main("ProdRoaming")
