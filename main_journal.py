import os
import time
import pandas
from src_journal import extended_df2_miner_apply


result = pandas.DataFrame(columns=["Log", "Runtime","Relations","Parameter"])


for file_name in os.listdir("data_local"):
    for parameter in [0.99,0.95,0.9,0.85,0.8,0.75,0.7,0.65,0.6,0.55,0.5]:
        start = time.time()
        try:
            eocpt = extended_df2_miner_apply("data_local/"+file_name,parameter)
            runtime = time.time()-start
            relations = eocpt.get_unique_relations()
            for entry in relations:
                print(entry)
            result.loc[result.shape[0]] = (file_name,runtime,relations,parameter)
            result.to_csv("result_journal.csv")
        except:
            print("Failure on ",file_name,parameter)
