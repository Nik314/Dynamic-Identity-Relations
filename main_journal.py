import os
import time

import pandas
from src_journal import extended_df2_miner_apply


for file_name in os.listdir("data"):
    start = time.time()
    print(extended_df2_miner_apply("data/"+file_name,0.95))
    print(time.time()-start)
    exit()

