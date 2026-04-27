import os
import time
import pandas

from src.oc_process_trees import OperatorNode
from src_journal import extended_df2_miner_apply

import json
import os

from src_journal.oc_process_trees import LeafNode


def write_tree_file(file_path,tree_string):
    with open(file_path,"w") as current_file:
        current_file.write(tree_string)


def load_tree_from_dict(tree_as_dict):
    if "subtrees" in tree_as_dict:
        return OperatorNode(operator=tree_as_dict["operator"],subtrees=[load_tree_from_dict(sub) for sub in tree_as_dict["subtrees"]])
    else:
        return LeafNode(activity=tree_as_dict["activity"],divergent=tree_as_dict["divergent"],
                        convergent=tree_as_dict["convergent"],deficient=tree_as_dict["deficient"],
                        related=tree_as_dict["related"])

def format_result():
    results = pandas.read_csv("result_journal.csv")
    results["Tree"] = results["Tree"].apply(lambda entry:str(load_tree_from_dict(eval(entry))))
    results["FileName"] = (results["Log"].apply(lambda entry:entry.split(".")[0])+"_"+
                    results["Parameter1"].apply(lambda p:str(p).split(".")[-1])+"_"
                    +results["Parameter2"].apply(lambda p:str(p).split(".")[-1]))
    results["FileName"] = "models_journal/"+results["FileName"]+".txt"
    results.apply(lambda row:write_tree_file(row["FileName"],row["Tree"]),axis=1)



format_result()
exit()

result = pandas.DataFrame(columns=["Log", "Runtime","Relations","Parameter1","Parameter2","Tree"])

for file_name in list(os.listdir("data"))[-2:]:
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




