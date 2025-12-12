import pandas


def get_interaction_patterns_noise(relations, noise_threshold):

    activities = relations["ocel:activity"].unique()
    types = relations["ocel:type"].unique()

    # Event → objects, Event → activity (duplicate-safe)
    evt_objects = relations.groupby("ocel:eid")["ocel:oid"].apply(lambda x: tuple(sorted(set(x))))
    evt_activity = relations.groupby("ocel:eid")["ocel:activity"].first()

    # Object → type
    obj_type = relations.groupby("ocel:oid")["ocel:type"].first()

    # Build event table
    identifiers = pandas.DataFrame({
        "activity": evt_activity.loc[evt_objects.index],
        "all": evt_objects
    }, index=evt_objects.index)

    # Add one column per object type
    for t in types:
        objs_of_t = obj_type[obj_type == t].index
        identifiers[t] = identifiers["all"].apply(
            lambda oids, ok=objs_of_t: tuple(oid for oid in oids if oid in ok)
        )

    # Prepare *probability* storage
    P_related    = {a: {} for a in activities}
    P_deficient  = {a: {} for a in activities}
    P_convergent = {a: {} for a in activities}
    P_divergent  = {a: {} for a in activities}

    # ----- Compute all probabilities -----------------------------------------

    for a in activities:

        events_a = identifiers[identifiers["activity"] == a]
        n_events = len(events_a)

        for t in types:

            # --- Relatedness ---------------------------------------------------
            appears = events_a[t].apply(len) > 0
            p_related = appears.mean() if n_events > 0 else 0.0
            P_related[a][t] = p_related

            # If not related → all others = 0
            if p_related == 0:
                P_deficient[a][t]  = 0.0
                P_convergent[a][t] = 0.0
                P_divergent[a][t]  = 0.0
                continue

            # --- Deficiency (instability of presence) -------------------------
            P_deficient[a][t] = 1 - abs(2 * p_related - 1)

            # --- Convergence ---------------------------------------------------
            p_convergent = (events_a[t].apply(len) > 1).mean()
            P_convergent[a][t] = p_convergent

            # --- Divergence ----------------------------------------------------
            contexts = {}
            for eid, row in events_a.iterrows():
                objs = row[t]
                if objs:
                    for o in objs:
                        contexts.setdefault(o, set()).add(row["all"])

            if len(contexts) == 0:
                P_divergent[a][t] = 0.0
            else:
                divergent_objs = sum(len(ctxs) > 1 for ctxs in contexts.values())
                P_divergent[a][t] = divergent_objs / len(contexts)

    related_sets    = {a: set() for a in activities}
    deficient_sets  = {a: set() for a in activities}
    convergent_sets = {a: set() for a in activities}
    divergent_sets  = {a: set() for a in activities}

    for a in activities:
        for t in types:

            if P_related[a][t] >= noise_threshold:
                related_sets[a].add(t)
            else:
                continue

            if P_deficient[a][t] >= noise_threshold:
                deficient_sets[a].add(t)

            if P_convergent[a][t] >= noise_threshold:
                convergent_sets[a].add(t)

            if P_divergent[a][t] >= noise_threshold:
                divergent_sets[a].add(t)

    return divergent_sets, convergent_sets, related_sets, deficient_sets

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