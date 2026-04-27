
import numpy
import pandas
import seaborn
import matplotlib.pyplot as plt
from sympy import false
import os
import pm4py

from src_journal import extended_df2_miner_apply

data = pandas.read_csv("result_journal.csv")
data["Noise Parameter Sum"] = (data["Parameter1"] + data["Parameter2"]).round(2).astype(str)
hue_order = sorted(data["Noise Parameter Sum"].unique(), key=float)
palette = {v: seaborn.color_palette("viridis_r", len(hue_order))[i] for i, v in enumerate(hue_order)}
fig, ax = plt.subplots(figsize=(16, 8))
ax = seaborn.stripplot(data, hue="Noise Parameter Sum", x="Log", y="Runtime", size=10, jitter=0.45, hue_order=hue_order, palette=palette, ax=ax)
plt.grid()
ax.set_xticks([0.5 + i for i in range(0, 12)])
ax.set_xticklabels([str(i) + "                 " for i in range(1, 11)]+["",""])
plt.savefig("runtime.png")
#plt.show()


