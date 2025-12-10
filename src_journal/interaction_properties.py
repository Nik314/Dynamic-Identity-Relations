import pandas



def get_interaction_patterns(relations):

    activities = relations["ocel:activity"].unique()
    types = relations["ocel:type"].unique()

    # SAFE: handle duplicate event IDs and object IDs
    evt_act = relations.groupby("ocel:eid")["ocel:activity"].first()
    obj_type = relations.groupby("ocel:oid")["ocel:type"].first()

    # Precompute event → tuple(sorted(oids))
    evt_objects = (
        relations.groupby("ocel:eid")["ocel:oid"]
        .apply(lambda x: tuple(sorted(set(x))))
    )

    # Build identifier table
    identifiers = pandas.DataFrame({
        "activity": evt_act.loc[evt_objects.index].values,
        "all": evt_objects
    }, index=evt_objects.index)

    # Precompute: event × object-type → tuple(oids-of-that-type)
    for t in types:
        object_ids_of_type = obj_type[obj_type == t].index
        identifiers[t] = identifiers["all"].apply(
            lambda oids, ok=object_ids_of_type: tuple(
                oid for oid in oids if oid in ok
            )
        )

    # Init result sets
    convergent = {a: set() for a in activities}
    divergent = {a: set() for a in activities}
    deficient = {a: set() for a in activities}
    related = {a: set(types.copy()) for a in activities}

    # ---- PHASE 1: deficient + related ---------------------------------------

    ct = relations.groupby(["ocel:activity", "ocel:type"])["ocel:eid"].nunique()
    total = relations.groupby("ocel:activity")["ocel:eid"].nunique()

    for a in activities:
        for t in types:
            c = ct.get((a, t), 0)
            if c == 0:
                related[a].discard(t)
            elif c < total[a]:
                deficient[a].add(t)

    # ---- PHASE 2: convergent + divergent ------------------------------------

    for t in types:

        # Only events where objects of this type appear
        sub = identifiers[identifiers[t].apply(len) > 0]

        for a, grp_a in sub.groupby("activity"):

            # convergent: event has >1 objects of type t
            if grp_a[t].apply(len).max() > 1:
                convergent[a].add(t)

            # divergent: same object-set → multiple distinct 'all'
            counts = grp_a.groupby(t)["all"].nunique()
            if counts.max() > 1:
                divergent[a].add(t)

    return divergent, convergent, related, deficient