
import numpy
import pandas
import seaborn
import matplotlib.pyplot as plt
from sympy import false

data = pandas.read_csv("result_journal.csv")
ax = seaborn.stripplot(data, hue="Parameter1", x="Log", y="Runtime", size=10, jitter=0.45)
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

def get_types(relation_string):
    recording = False
    result = []
    for c in relation_string:
        if recording:
            result[-1] += c
        if c == "'" and recording:
            recording = False
        elif c =="'" and not recording:
            recording = True
            result.append("")
    return result


def average_count(relation_string):
    result = [relation_string.count(ot) for ot in get_types(relation_string)]
    return sum(result) if result else 0


data["Count"] = data["Relations"].apply(lambda entry:average_count(entry))
data["Parameter"] = data["Parameter1"] + data["Parameter2"]
data["Log"] = data["Log"].apply(lambda entry:entry.split("_")[0])
ax = seaborn.stripplot(data, hue="Parameter2", x="Log", y="Count", size=10, jitter=0.45)
plt.grid()
ax.set_xticks([0.5 + i for i in range(0, 10)])
ax.set_xticklabels([str(i) + "                 " for i in range(1, 11)])
plt.savefig("rank.png")
plt.show()
