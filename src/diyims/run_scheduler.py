from diyims.scheduler import scheduler_main
from multiprocessing import set_start_method, freeze_support
import os

freeze_support()
set_start_method("spawn")
roaming = "Roaming"
os.environ["DIYIMS_ROAMING"] = roaming

scheduler_main(roaming)
