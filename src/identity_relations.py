from src.oc_process_trees import LeafNode,OperatorNode
import hashlib




def check_relation(ot1, ot2, relations):


    hash_map = relations.groupby("ocel:eid").apply(lambda frame:
        int(hashlib.md5(str(sorted(list(frame["ocel:oid"].unique()))).encode("utf_8")).hexdigest(), 16)).to_dict()

    relations["hash"] = relations["ocel:eid"].apply(lambda eid: hash_map[eid])

    if relations.groupby("ocel:oid").nunique()["hash"].max() == 1:
        return "Sync " + str(ot1) + " With " + str(ot2)


    if relations[relations["ocel:type"].isin(ot1)].groupby("ocel:oid").nunique()["hash"].max() > 1:
        return None

    ot1_hash_map = relations[relations["ocel:type"].isin(ot1)].groupby("ocel:eid").apply(lambda frame:
         int(hashlib.md5(str(sorted(list(frame["ocel:oid"].unique()))).encode("utf_8")).hexdigest(), 16)).to_dict()

    relations["ot1 hash"] = relations["ocel:eid"].apply(lambda eid: ot1_hash_map[eid] if eid in ot1_hash_map else
        int(hashlib.md5(str([]).encode("utf_8")).hexdigest(), 16))
    hash_groups = relations.groupby("ot1 hash").apply(lambda frame:frame["hash"].unique()).to_dict().values()
    hash_groups = [group for group in hash_groups if len(group) > 1]

    for group in hash_groups:
        sub_relations = relations[relations["hash"].isin(group)]
        time_frames = sub_relations.groupby("hash").apply(lambda frame:(frame["ocel:timestamp"].iloc[0],frame["ocel:timestamp"].iloc[-1])).to_dict().values()

        for frame_1 in time_frames:
            for frame_2 in time_frames:
                if frame_1[0] < frame_2[0] and frame_1[1] < frame_2[0]:
                    continue
                if frame_1[0] > frame_2[1] and frame_1[1] > frame_2[1]:
                    continue
                return "Imp " + str(ot1) + " With " + str(ot2) + " (Concurrent)"

    return "Imp " + str(ot1) + " With " + str(ot2) + " (Ordered)"


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
                operator = check_relation(ot1, ot2, sub_log)
                if operator:
                    candidates = [ots for ots in candidates if ots != ot1 and ots != ot2] + [ot1 | ot2]
                    return OperatorNode(operator, [get_extended_ocpt(ocpt,relations,candidates)])

        return OperatorNode(ocpt.operator,[get_extended_ocpt(sub,relations, candidates) for sub in ocpt.subtrees])

