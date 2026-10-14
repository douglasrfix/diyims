import os
from diyims.general_utils import clean_up

roaming = "DevRoaming"
os.environ["DIYIMS_ROAMING"] = roaming
if __name__ == '__main__':
    clean_up(roaming)
