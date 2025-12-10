from src_journal.oc_process_trees import LeafNode,OperatorNode
import hashlib
import pandas as pd
import numpy as np



def check_relation_fast_activity_subsets(ot1, ot2, relations, activity_subsets=None):

    relations = relations.copy()

    # Ensure ot1 and ot2 are lists
    if isinstance(ot1, str):
        ot1 = [ot1]
    if isinstance(ot2, str):
        ot2 = [ot2]

    # --- 1. Filter only relevant object types ---
    relevant_mask = relations["ocel:type"].isin(ot1 | ot2)
    relevant_relations = relations[relevant_mask]
    if relevant_relations.empty:
        return None  # trivial, no events with ot1 or ot2

    # --- 2. If activity_subsets is provided, iterate subsets for SYNC ---
    subsets_to_check = activity_subsets if activity_subsets is not None else [
        relevant_relations["ocel:activity"].unique()]

    # Helper: compute hash of sorted tuple of object IDs
    def tuple_hash(oids):
        return hashlib.md5(str(tuple(sorted(oids))).encode()).hexdigest()

    for activity_subset in subsets_to_check:
        subset_events = relevant_relations[relevant_relations["ocel:activity"].isin(activity_subset)]
        if subset_events.empty:
            continue

        # Count co-occurrences: only events that have both ot1 and ot2
        event_groups = subset_events.groupby("ocel:eid")["ocel:type"].agg(set)
        cooccur_events = event_groups[event_groups.apply(lambda s: bool(set(ot1) & s) and bool(set(ot2) & s))]
        if cooccur_events.empty:
            continue  # trivial in this subset

        # --- 3. Compute full event hashes within this subset ---
        evt_hash = subset_events.groupby("ocel:eid")["ocel:oid"].agg(lambda x: tuple_hash(set(x)))
        subset_events["hash"] = subset_events["ocel:eid"].map(evt_hash)

        # --- 4. Check strict SYNC in this subset ---
        if subset_events.groupby("ocel:oid")["hash"].nunique().max() == 1:
            return f"Sync {ot1} With {ot2}"

        # --- 5. Check if ot1 is trivial (varies in multiple contexts) ---
        ot1_mask = subset_events["ocel:type"].isin(ot1)
        if subset_events[ot1_mask].groupby("ocel:oid")["hash"].nunique().max() > 1:
            continue  # trivial in this subset

        # --- 6. Compute ot1-only hashes ---
        ot1_evt_hash = subset_events[ot1_mask].groupby("ocel:eid")["ocel:oid"].agg(lambda x: tuple_hash(set(x)))
        subset_events["ot1_hash"] = subset_events["ocel:eid"].map(ot1_evt_hash).fillna(tuple_hash([]))

        # --- 7. Group by ot1_hash, keep groups with multiple full hashes ---
        hash_groups = (
            subset_events.groupby("ot1_hash")["hash"]
            .unique()
            .apply(list)
            .to_list()
        )
        hash_groups = [g for g in hash_groups if len(g) > 1]
        if not hash_groups:
            return f"Imp {ot1} With {ot2} (Ordered)"

        # --- 8. Prepare start/end timestamps per hash ---
        hash_times = (
            subset_events.groupby("hash")["ocel:timestamp"]
            .agg(['min', 'max'])
            .rename(columns={'min': 'start', 'max': 'end'})
        )

        # --- 9. Check concurrency ---
        for group in hash_groups:
            times = hash_times.loc[group].to_numpy()
            start = times[:, 0][:, None]
            end = times[:, 1][:, None]
            overlap = (start < end.T) & (end > start.T)
            np.fill_diagonal(overlap, False)
            if overlap.any():
                return f"Imp {ot1} With {ot2} (Concurrent)"

        # --- 10. Otherwise Ordered ---
        return f"Imp {ot1} With {ot2} (Ordered)"

    # If none of the activity subsets produced a relation → trivial
    return None





def get_extended_ocpt(ocpt, relations, candidates=None):

    if isinstance(ocpt,LeafNode):
        return ocpt

    else:
        if not candidates:
            candidates = [{ot} for ot in relations["ocel:type"].unique()]

        activities = ocpt.get_activities()
        for ot1 in candidates:
            for ot2 in candidates:
                if ot1 == ot2:
                    continue
                sub_log = relations[relations["ocel:type"].isin(ot1|ot2) & relations["ocel:activity"].isin(activities)]
                operator = check_relation_fast_activity_subsets(ot1, ot2, sub_log)
                if operator:
                    candidates = [ots for ots in candidates if ots != ot1 and ots != ot2] + [ot1 | ot2]
                    return OperatorNode(operator, [get_extended_ocpt(ocpt,relations,candidates)])

        return OperatorNode(ocpt.operator,[get_extended_ocpt(sub,relations, candidates) for sub in ocpt.subtrees])

