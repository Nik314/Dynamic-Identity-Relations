import pm4py
import time
import pandas
import warnings
warnings.simplefilter(action="ignore", category=pandas.errors.SettingWithCopyWarning)
from src.interaction_properties import get_interaction_patterns
from src.divergence_free_graph import get_divergence_free_graph
from src.oc_process_trees import load_from_pt
from src.identity_relations import get_extended_ocpt



def df2_miner_apply(log_path):

    try:
        input_log = pm4py.read_ocel2(log_path).relations
    except:
        input_log = pm4py.read_ocel(log_path).relations

    div, con, rel, defi = get_interaction_patterns(input_log)
    print("Interacting Properties Done")
    df2_graph = get_divergence_free_graph(input_log,div,rel)
    print("DF2 Graph Done")
    process_tree = pm4py.discover_process_tree_inductive(df2_graph, noise_threshold=0.2)
    print("Traditional Process Tree Done")
    ocpt = load_from_pt(process_tree,rel,div,con,defi)
    print("Object-Centric Process Tree Done")
    start = time.time()
    extended_ocpt = get_extended_ocpt(ocpt, input_log)
    runtime = time.time() - start
    print("Identity Relations Done")
    return extended_ocpt, runtime



