def main(roaming: str) -> None:
    import os
    import uvicorn

    os.environ["ROAMING"] = str(roaming)
    uvicorn.run("diyims.fastapi_app:myapp", host="0.0.0.0", port=8000)

    return


if __name__ == "__main__":
    main("Roaming")
