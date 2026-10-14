import os

from diyims.clean_up import clean_up

roaming = "Roaming"
os.environ["DIYIMS_ROAMING"] = roaming

clean_up(roaming)
