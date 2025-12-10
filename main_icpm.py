import os
import pandas
from src import df2_miner_apply


result = pandas.DataFrame(columns=["Log", "Runtime","Sync", "Imp (Ordered)", "Imp (Concurrent)"])

for file_name in os.listdir("data"):
    ocpt, runtime = df2_miner_apply("data/"+file_name)
    relations = ocpt.get_unique_relations()
    print(ocpt)
    sync = len([rel for rel in relations if "syn" in rel.lower()])
    ordered = len([rel for rel in relations if "ordered" in rel.lower()])
    concurrent = len([rel for rel in relations if "concurrent" in rel.lower()])
    result.loc[result.shape[0]] = (file_name.split(".")[0], runtime,sync,ordered,concurrent)
    print(result)
#result.to_csv("results.csv")


