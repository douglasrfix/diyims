import os
from diyims.general_utils import clean_up

roaming = "Roaming"
os.environ["DIYIMS_ROAMING"] = roaming

clean_up(roaming)
