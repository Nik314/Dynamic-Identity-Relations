import os
import time

from src_journal import extended_df2_miner_apply


for file_name in reversed(os.listdir("data")):
    start = time.time()
    print(extended_df2_miner_apply("data/"+file_name,0.99))
    print(time.time()-start)
    exit()

