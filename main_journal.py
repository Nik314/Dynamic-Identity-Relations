import os
import time
import pandas
from src_journal import extended_df2_miner_apply


result = pandas.DataFrame(columns=["Log", "Runtime","Relations","Parameter1","Parameter2","Tree"])

for file_name in os.listdir("data"):
    for parameter1 in [0.99,0.9,0.8,0.7,0.6,0.5]:
        for parameter2 in [0.99,0.9,0.8,0.7,0.6,0.5]:
            start = time.time()
            try:
                eocpt = extended_df2_miner_apply("data/"+file_name,parameter1,parameter2)
                runtime = time.time()-start
                relations = eocpt.get_all_relations()
                for entry in relations:
                    print(entry)
                result.loc[result.shape[0]] = (file_name,runtime,relations,parameter1,parameter2,eocpt.get_as_dict())
                result.to_csv("result_journal.csv")
            except:
                print("Failure on ",file_name,parameter1,parameter2)
