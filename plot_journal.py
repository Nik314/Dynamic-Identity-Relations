
import numpy
import pandas
import seaborn
import matplotlib.pyplot as plt
from sympy import false
import os
import pm4py




data = pandas.read_csv("result_journal.csv")
data["Noise Parameter Sum"] = data["Parameter1"] + data["Parameter2"]
ax = seaborn.stripplot(data, hue="Noise Parameter Sum", x="Log", y="Runtime", size=10, jitter=0.45)
plt.grid()
ax.set_xticks([0.5 + i for i in range(0, 10)])
ax.set_xticklabels([str(i) + "                 " for i in range(1, 11)])
plt.savefig("runtime.png")
plt.show()



strict_rank = {
    "strict":0,
   "partition":1,
   "overlap":2,
  "order":3,
    "batch":4,
    "concurrent":5
}


def evaluate_relations_average_coverage(relation_string):
    relation = eval(relation_string)
    result = {key:[] for key in strict_rank.keys()}
    for operator, activities in relation:
        for key in result.keys():
            if key in operator.lower():
                result[key].append(len(activities))
    return sum([(sum(result[key]) / len(result[key]) if result[key] else 0) for key in result.keys()]) / 6


def evaluate_relations_per_kind(relation_string,key):
    relation = eval(relation_string)
    return len([operator for operator,activities in relation if key in operator.lower()])


def evaluate_relation_count(relation_string):
    relation = eval(relation_string)
    result = {operator for operator,activities in relation}
    return len(result)

data["Average Activity Coverage"] = data["Relations"].apply(lambda entry:evaluate_relations_average_coverage(entry))
data["Average Relation Count"] = data["Relations"].apply(lambda entry:evaluate_relation_count(entry))



for key in strict_rank.keys():
    print(key)
    data[key] = data["Relations"].apply(lambda entry: evaluate_relations_per_kind(entry, key))
    print(data.groupby("Parameter1")[key].mean())
    print(data.groupby("Parameter2")[key].mean())

print(data.groupby("Noise Parameter Sum")["Average Relation Count"].mean())
print(data.groupby("Parameter1")["Average Relation Count"].mean())
print(data.groupby("Parameter2")["Average Relation Count"].mean())





