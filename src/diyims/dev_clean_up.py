import os

from diyims.clean_up import clean_up

roaming = "DevRoaming"
os.environ["DIYIMS_ROAMING"] = roaming
if __name__ == '__main__':
    clean_up(roaming)
