import uvicorn


if __name__ == "__main__":
    uvicorn.run("diyims.main:myapp", host="0.0.0.0", port=8000)
