import pm4py
import pandas
import warnings

warnings.simplefilter(action="ignore", category=pandas.errors.SettingWithCopyWarning)
from src_journal.interaction_properties import get_interaction_patterns_noise
from src_journal.divergence_free_graph import get_divergence_free_graph
from src_journal.oc_process_trees import load_from_pt
from src_journal.identity_relations import get_extended_ocpt
from src_journal.tree_normal_form import create_candidate_set

def extended_df2_miner_apply(log_path,noise_treshold):

    try:
        input_log = pm4py.read_ocel2(log_path).relations
    except:
        input_log = pm4py.read_ocel(log_path).relations

    #filtering of the activity nodes
    activity_count = input_log.groupby("ocel:activity")["ocel:eid"].nunique().to_dict()
    sorted_counts = list(reversed(sorted(activity_count.values())))
    cutoff = min([len(sorted_counts)] + [i for i in range(0,len(sorted_counts))
        if sum(sorted_counts[:i]) >= sum(sorted_counts)*noise_treshold])
    allowed_counts = sorted_counts[:cutoff]
    allowed_activities = [a for a,v in activity_count.items() if v in allowed_counts]
    input_log = input_log[input_log["ocel:activity"].isin(allowed_activities)]

    #filtering multiplicity properties
    div, con, rel, defi = get_interaction_patterns_noise(input_log,1-noise_treshold)
    print("Interacting Properties Done")

    #extracting df2 graph
    df2_graph = get_divergence_free_graph(input_log,div,rel)
    print("DF2 Graph Done")

    #filtering on the df2 graph edges
    process_tree = pm4py.discover_process_tree_inductive(df2_graph,noise_threshold=1-noise_treshold)
    print("Traditional Process Tree Done")

    #creating relation free OCPT
    ocpt = load_from_pt(process_tree,rel,div,con,defi)
    print("Object-Centric Process Tree Done")

    #create candidate set
    candidates = create_candidate_set(ocpt)

    #extend each candidate tree
    extended_candidates = [get_extended_ocpt(tree,input_log) for tree in candidates]

    #select extended candidate with the most relations
    result = extended_candidates[0]
    for tree in extended_candidates[1:]:
        if tree.get_unique_relations() > result.get_unique_relations():
            result = tree

    return result






