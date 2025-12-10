import pm4py
import pandas
import warnings
import time
warnings.simplefilter(action="ignore", category=pandas.errors.SettingWithCopyWarning)
from src_journal.interaction_properties import get_interaction_patterns
from src_journal.divergence_free_graph import get_divergence_free_graph
from src_journal.oc_process_trees import load_from_pt


def extended_df2_miner_apply(log_path,noise_treshold):

    try:
        input_log = pm4py.read_ocel2(log_path).relations
    except:
        input_log = pm4py.read_ocel(log_path).relations


    activity_count = input_log.groupby("ocel:activity")["ocel:eid"].nunique().to_dict()
    sorted_counts = list(reversed(sorted(activity_count.values())))
    cutoff = min([i for i in range(0,len(sorted_counts))
        if sum(sorted_counts[:i]) >= sum(sorted_counts)*noise_treshold])
    allowed_counts = sorted_counts[:cutoff]
    allowed_activities = [a for a,v in activity_count.items() if v in allowed_counts]
    input_log = input_log[input_log["ocel:activity"].isin(allowed_activities)]
    print(allowed_activities)


    div, con, rel, defi = get_interaction_patterns(input_log)
    print("Interacting Properties Done")
    df2_graph = get_divergence_free_graph(input_log,div,rel)
    print("DF2 Graph Done")
    process_tree = pm4py.discover_process_tree_inductive(df2_graph)
    print("Traditional Process Tree Done")
    ocpt = load_from_pt(process_tree,rel,div,con,defi)
    print("Object-Centric Process Tree Done")
    print("Extend Tree (Todo)")
    return ocpt





