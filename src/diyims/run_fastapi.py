import subprocess


def main(roaming: str) -> None:
    # TODO: improve path function

    subprocess.run(["fastapi", "dev", "fastapi_app.py"])
    return


if __name__ == "__main__":
    main("Roaming")
