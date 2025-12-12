from src_journal.oc_process_trees import LeafNode,OperatorNode
from src_journal.tree_normal_form import *
from collections import defaultdict
import math
from collections import defaultdict
from itertools import combinations

def check_strict_sync(relations, ot1, ot2, violation_threshold):

    df = relations.copy()

    grouped = df.groupby("ocel:eid")
    event_sets = grouped.agg({
        "ocel:oid": lambda x: frozenset(x),
        "ocel:type": lambda x: list(x)
    })

    event_sets["ot1_set"] = event_sets.apply(
        lambda row: frozenset([oid for oid, t in zip(row["ocel:oid"], row["ocel:type"]) if t in ot1]), axis=1
    )
    event_sets["ot2_set"] = event_sets.apply(
        lambda row: frozenset([oid for oid, t in zip(row["ocel:oid"], row["ocel:type"]) if t in ot2]), axis=1
    )

    event_sets = event_sets[(event_sets["ot1_set"]) | (event_sets["ot2_set"])]
    if event_sets.empty:
        return True

    violating_sets = set()
    all_sets = set()

    ot1_to_ot2 = {}
    ot2_to_ot1 = {}

    for s1, s2 in zip(event_sets["ot1_set"], event_sets["ot2_set"]):
        if s1:
            all_sets.add(s1)
            ot1_to_ot2.setdefault(s1, set()).add(s2)
        if s2:
            all_sets.add(s2)
            ot2_to_ot1.setdefault(s2, set()).add(s1)

    for s1, mapped in ot1_to_ot2.items():
        if len(mapped) > 1:
            violating_sets.add(s1)
    for s2, mapped in ot2_to_ot1.items():
        if len(mapped) > 1:
            violating_sets.add(s2)

    from collections import defaultdict

    obj_to_ot1_sets = defaultdict(set)
    obj_to_ot2_sets = defaultdict(set)

    for s1 in event_sets["ot1_set"]:
        for oid in s1:
            obj_to_ot1_sets[oid].add(s1)
    for s2 in event_sets["ot2_set"]:
        for oid in s2:
            obj_to_ot2_sets[oid].add(s2)

    for sets in obj_to_ot1_sets.values():
        if len(sets) > 1:
            violating_sets.update(sets)
    for sets in obj_to_ot2_sets.values():
        if len(sets) > 1:
            violating_sets.update(sets)

    if not all_sets:
        return True

    violation_fraction = len(violating_sets) / len(all_sets)
    return violation_fraction <= violation_threshold


def check_subset_sync( relations,ot1, ot2, strict_activities,  relaxed_activities, violation_threshold):

    df = relations.copy()

    grouped = df.groupby("ocel:eid")
    event_sets = grouped.agg({
        "ocel:oid": lambda x: frozenset(x),
        "ocel:type": lambda x: list(x),
        "ocel:activity": "first"
    })

    event_sets["ot1_set"] = event_sets.apply(
        lambda row: frozenset([oid for oid, t in zip(row["ocel:oid"], row["ocel:type"]) if t in ot1]), axis=1
    )
    event_sets["ot2_set"] = event_sets.apply(
        lambda row: frozenset([oid for oid, t in zip(row["ocel:oid"], row["ocel:type"]) if t in ot2]), axis=1
    )

    event_sets = event_sets[(event_sets["ot1_set"].apply(len) > 0) | (event_sets["ot2_set"].apply(len) > 0)]
    if event_sets.empty:
        return True

    violating_sets = set()
    all_sets = set()

    strict_es = event_sets[event_sets["ocel:activity"].isin(strict_activities)]
    if not strict_es.empty:
        ot1_to_ot2 = {}
        ot2_to_ot1 = {}
        for s1, s2 in zip(strict_es["ot1_set"], strict_es["ot2_set"]):
            all_sets.add(s1)
            all_sets.add(s2)
            ot1_to_ot2.setdefault(s1, set()).add(s2)
            ot2_to_ot1.setdefault(s2, set()).add(s1)

        for s1, mapped in ot1_to_ot2.items():
            if len(mapped) > 1:
                violating_sets.add(s1)
        for s2, mapped in ot2_to_ot1.items():
            if len(mapped) > 1:
                violating_sets.add(s2)

    relaxed_es = event_sets[event_sets["ocel:activity"].isin(relaxed_activities)]
    if not relaxed_es.empty and not strict_es.empty:
        strict_map = {s1: s2 for s1, s2 in zip(strict_es["ot1_set"], strict_es["ot2_set"])}
        for s1, s2 in zip(relaxed_es["ot1_set"], relaxed_es["ot2_set"]):
            all_sets.add(s1)
            all_sets.add(s2)
            if s1 not in strict_map:
                violating_sets.add(s1)
                continue
            if not s2 <= strict_map[s1]:
                violating_sets.add(s2)

    if not all_sets:
        return True

    violation_fraction = len(violating_sets) / len(all_sets)
    return violation_fraction <= violation_threshold




def check_subset_overlap(relations, ot1, ot2, violation_threshold):

    df = relations.copy()

    grouped = df.groupby("ocel:eid")
    event_sets = grouped.agg({
        "ocel:oid": lambda x: list(x),
        "ocel:type": lambda x: list(x)
    })

    def get_ot_set(oids, types, target_types):
        return frozenset([oid for oid, t in zip(oids, types) if t in target_types])

    event_sets["ot1_set"] = [get_ot_set(oids, types, ot1) for oids, types in
                             zip(event_sets["ocel:oid"], event_sets["ocel:type"])]
    event_sets["ot2_set"] = [get_ot_set(oids, types, ot2) for oids, types in
                             zip(event_sets["ocel:oid"], event_sets["ocel:type"])]

    event_sets = event_sets[(event_sets["ot1_set"].apply(len) > 0) & (event_sets["ot2_set"].apply(len) > 0)]
    if event_sets.empty:
        return True

    ot1_to_ot2_sets = defaultdict(list)
    for s1, s2 in zip(event_sets["ot1_set"], event_sets["ot2_set"]):
        ot1_to_ot2_sets[s1].append(s2)

    violating_ot1_sets = set()
    all_ot1_sets = set(ot1_to_ot2_sets.keys())

    for s1, ot2_list in ot1_to_ot2_sets.items():
        if len(ot2_list) <= 1:
            continue
        for a, b in combinations(ot2_list, 2):
            if a & b:
                violating_ot1_sets.add(s1)
                break

    violation_fraction = len(violating_ot1_sets) / len(all_ot1_sets)
    return violation_fraction <= violation_threshold



def check_implication(relations, ot1, ot2, violation_threshold):

    df = relations.copy()

    if "ocel:oid" not in df.columns or "ocel:type" not in df.columns:
        raise ValueError("DataFrame must contain 'ocel:oid' and 'ocel:type' columns.")

    grouped = df.groupby("ocel:eid")
    event_sets = grouped.agg({
        "ocel:oid": lambda x: list(x),
        "ocel:type": lambda x: list(x)
    })

    def get_ot_set(oids, types, target_types):
        return frozenset([oid for oid, t in zip(oids, types) if t in target_types])

    event_sets["ot1_set"] = [get_ot_set(oids, types, ot1) for oids, types in
                             zip(event_sets["ocel:oid"], event_sets["ocel:type"])]
    event_sets["ot2_set"] = [get_ot_set(oids, types, ot2) for oids, types in
                             zip(event_sets["ocel:oid"], event_sets["ocel:type"])]

    event_sets = event_sets[[len(s) > 0 for s in event_sets["ot1_set"]]]
    if event_sets.empty:
        return True

    ot1_to_ot2 = defaultdict(set)
    all_sets = set()
    violating_sets = set()

    for s1, s2 in zip(event_sets["ot1_set"], event_sets["ot2_set"]):
        all_sets.add(s1)
        if s2:
            all_sets.add(s2)
        ot1_to_ot2[s1].add(s2)

    for s1, mapped in ot1_to_ot2.items():
        if len(mapped) > 1:
            violating_sets.add(s1)
            violating_sets.update(mapped)

    if not all_sets:
        return True

    violation_fraction = len(violating_sets) / len(all_sets)
    return violation_fraction <= violation_threshold



def check_implication_k(relations, ot1, ot2, violation_threshold=0.0):

    df = relations.copy()

    df_ot1 = df[df["ocel:type"].isin(ot1)][["ocel:eid", "ocel:oid", "ocel:timestamp"]]
    df_ot2 = df[df["ocel:type"].isin(ot2)][["ocel:eid", "ocel:oid"]]

    if df_ot1.empty or df_ot2.empty:
        return 0

    eid_to_ot1 = df_ot1.groupby("ocel:eid")["ocel:oid"].agg(frozenset).to_dict()
    ot1_to_interval = df_ot1.groupby("ocel:oid")["ocel:timestamp"].agg(["min", "max"]).to_dict(orient="index")
    eid_to_ot2 = df_ot2.groupby("ocel:eid")["ocel:oid"].agg(frozenset).to_dict()

    ot2_to_ot1_objs = defaultdict(set)
    for eid, ot2_objs in eid_to_ot2.items():
        ot1_objs = eid_to_ot1.get(eid, frozenset())
        for o2 in ot2_objs:
            ot2_to_ot1_objs[o2].update(ot1_objs)

    concurrency_list = []

    for ot2_obj, ot1_objs in ot2_to_ot1_objs.items():
        intervals = []
        for o1 in ot1_objs:
            ts = ot1_to_interval.get(o1)
            if ts:
                intervals.append((ts["min"], ts["max"]))
        if not intervals:
            concurrency_list.append(0)
            continue
        intervals.sort(key=lambda x: x[0])
        max_concurrent = 1
        end_prev = intervals[0][1]
        for start, end in intervals[1:]:
            if start <= end_prev:
                max_concurrent += 1
            else:
                max_concurrent = 1
            end_prev = max(end_prev, end)
        concurrency_list.append(max_concurrent)

    if not concurrency_list:
        return 0

    concurrency_list.sort(reverse=True)
    n = len(concurrency_list)
    allowed_violations = int(n * violation_threshold)

    k_min = concurrency_list[allowed_violations] if allowed_violations < n else 0
    if (k_min > relations[relations["ocel:type"].isin(ot1)]["ocel:oid"].nunique() /
            relations[relations["ocel:type"].isin(ot2)]["ocel:oid"].nunique()):
        return math.inf
    return k_min


def insert_subset_sync(ocpt, ot1, ot2, strict, sub, overlap, root=True):

    if root:
        return OperatorNode(str(ot1) + " Strict Synchronization" + str(ot2),
            subtrees=[ OperatorNode(ocpt.Operator,[insert_subset_sync(tree,ot1,ot2,strict,sub,overlap,False) for tree in ocpt.subtrees])])

    elif isinstance(ocpt,OperatorNode) and all(a in sub for a in ocpt.get_activities()):
        return OperatorNode(str(ot1) + " Overlap" if overlap else " Partition" +" Subset Synchronization" + str(ot2),
                     subtrees=[ocpt])
    elif isinstance(ocpt, OperatorNode):
        return OperatorNode(ocpt.operator, [insert_subset_sync(tree, ot1, ot2, strict, sub, overlap,False) for tree in ocpt.subtrees])
    else:
        return OperatorNode(str(ot1) + " Overlap" if overlap else " Partition" +" Subset Synchronization" + str(ot2),subtrees=[ocpt])



def check_relation(ot1, ot2, relations,noise_threshold):

    sync_result = check_strict_sync(relations, ot1, ot2,noise_threshold)
    if sync_result:
        return str(ot1) +  " Strict Synchronization "+str(ot2), None, None, None

    activities_todo = set(relations["ocel:activity"].unique())
    cluster = [{activities_todo.pop()}]
    for a in activities_todo:
        check = False
        for index in range(len(cluster)):
            sublog = relations[relations["ocel:activity"].isin(cluster[index] | {a})]
            if check_strict_sync(sublog,ot1,ot2,noise_threshold):
                cluster[index] = cluster[index] | {a}
                check = True
                break
        if not check:
            cluster+=[{a}]

    candidates = [(sync,{a for a in relations["ocel:activity"].unique() if a not in sync}) for sync in cluster]

    for strict, sub in candidates:
        check = check_subset_sync(relations,ot1,ot2,strict,sub,noise_threshold)
        if check:
            return str(ot1) + " Strict Synchronization" + str(ot2), strict, sub, check_subset_overlap(relations,ot1,ot2,noise_threshold)

    check = check_implication(relations,ot1,ot2,noise_threshold)
    if check:
        k_min = check_implication_k(relations,ot1,ot2,noise_threshold)
        if k_min == 1:
            return str(ot1) + " Ordered Implication " + str(ot2), None, None, None
        elif k_min == math.inf:
            return str(ot1) + " Concurrent Implication " + str(ot2), None, None, None
        else:
            return str(ot1) + f" {k_min}-Batch Implication) " + str(ot2), None, None, None

    return None, None, None, None



def get_extended_ocpt_old(ocpt, relations, candidates=None,noise_threshold = 0.0, subset_needed = False):

    if subset_needed:
        subset_needed = not "subset" in ocpt.operator
        return OperatorNode(ocpt.operator,[get_extended_ocpt(sub,relations, candidates,noise_threshold, subset_needed) for sub in ocpt.subtrees])


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
                operator, strict, sub, overlap = check_relation(ot1, ot2, sub_log,noise_threshold)

                if operator:
                    candidates = [ots for ots in candidates if ots != ot1 and ots != ot2] + [ot1 | ot2]
                    if strict and sub:
                        subset_needed = True
                        ocpt = insert_subset_sync(ocpt, ot1, ot2,strict,sub, overlap)

                    return OperatorNode(operator, [get_extended_ocpt(ocpt,relations,candidates,noise_threshold, subset_needed)])

        return OperatorNode(ocpt.operator,[get_extended_ocpt(sub,relations, candidates,noise_threshold, subset_needed) for sub in ocpt.subtrees])




def get_extended_ocpt(ocpt, relations, candidates=None, noise_threshold=0.0, subset_needed=False):

    if subset_needed:
        subset_needed = not "subset" in ocpt.operator
        return OperatorNode(
            ocpt.operator,
            [get_extended_ocpt(sub, relations, candidates, noise_threshold, subset_needed) for sub in ocpt.subtrees]
        )

    if isinstance(ocpt, LeafNode):
        return ocpt

    if not candidates:
        candidates = [{ot} for ot in relations["ocel:type"].unique()]

    activities = ocpt.get_activities()
    relation_types = ["strict_sync", "subset_sync", "implication"]

    for relation_type in relation_types:
        for ot1 in candidates:
            for ot2 in candidates:
                if ot1 == ot2:
                    continue

                sub_log = relations[relations["ocel:type"].isin(ot1 | ot2) &
                                    relations["ocel:activity"].isin(activities)]

                operator, strict, sub, overlap = None, None, None, None
                if relation_type == "strict_sync":
                    if check_strict_sync(sub_log, ot1, ot2, noise_threshold):
                        operator = str(ot1) + " Strict Synchronization " + str(ot2)

                elif relation_type == "subset_sync":
                    # Inline subset sync logic to avoid recursion
                    activities_todo = set(sub_log["ocel:activity"].unique())
                    if activities_todo:
                        cluster = [{activities_todo.pop()}]
                        for a in activities_todo:
                            added = False
                            for i in range(len(cluster)):
                                subsub_log = sub_log[sub_log["ocel:activity"].isin(cluster[i] | {a})]
                                if check_strict_sync(subsub_log, ot1, ot2, noise_threshold):
                                    cluster[i] |= {a}
                                    added = True
                                    break
                            if not added:
                                cluster.append({a})

                        candidates_clusters = [(sync, {a for a in sub_log["ocel:activity"].unique() if a not in sync}) for sync in cluster]
                        for strict_set, relaxed_set in candidates_clusters:
                            if check_subset_sync(sub_log, ot1, ot2, strict_set, relaxed_set, noise_threshold):
                                overlap = check_subset_overlap(sub_log, ot1, ot2, noise_threshold)
                                operator = str(ot1) + " Strict Synchronization " + str(ot2)
                                strict, sub = strict_set, relaxed_set
                                break

                elif relation_type == "implication":
                    if check_implication(sub_log, ot1, ot2, noise_threshold):
                        k_min = check_implication_k(sub_log, ot1, ot2, noise_threshold)
                        if k_min == 1:
                            operator = str(ot1) + " Ordered Implication " + str(ot2)
                        elif k_min == math.inf:
                            operator = str(ot1) + " Concurrent Implication " + str(ot2)
                        else:
                            operator = str(ot1) + f" {k_min}-Batch Implication " + str(ot2)

                if operator:
                    print(operator)
                    candidates = [c for c in candidates if c != ot1 and c != ot2] + [ot1 | ot2]

                    if strict and sub:
                        subset_needed = True
                        ocpt = insert_subset_sync(ocpt, ot1, ot2, strict, sub, overlap)

                    return OperatorNode(
                        operator,
                        [get_extended_ocpt(ocpt, relations, candidates, noise_threshold, subset_needed)]
                    )

                else:
                    print("Nothing found for ",relation_type)
    return OperatorNode(
        ocpt.operator,
        [get_extended_ocpt(sub, relations, candidates, noise_threshold, subset_needed) for sub in ocpt.subtrees]
    )