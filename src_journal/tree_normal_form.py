import operator

from src_journal.oc_process_trees import *



def get_related_types(ocpt):
    if isinstance(ocpt,LeafNode):
        if ocpt.activity != "":
            return ocpt.related
        else:
            return set()
    return set([ot for sub in ocpt.subtrees for ot in get_related_types(sub)])


def get_divergent_types(ocpt):
    if isinstance(ocpt,LeafNode):
        if ocpt.activity != "":
            return ocpt.divergent
        else:
            return set()

    candidates = set([ot for sub in ocpt.subtrees for ot in get_related_types(sub)])
    return {ot for ot in candidates if all (ot not in get_related_types(sub)
        or ot in get_divergent_types(sub) for sub in ocpt.subtrees)}



def reduction_rule_one(ocpt):

    if isinstance(ocpt,LeafNode) or ocpt.operator != Operator.PARALLEL:
        return ocpt, False

    relevant_subtrees = [sub for sub in ocpt.subtrees
        if isinstance(sub,OperatorNode) and sub.operator == Operator.PARALLEL]

    if relevant_subtrees:
        return OperatorNode(Operator.PARALLEL,subtrees=[sub for sub in ocpt.subtrees if sub != relevant_subtrees[0]]+
                            [sub for sub in relevant_subtrees[0].subtrees]),True

    return ocpt,False



def reduction_rule_two(ocpt):
    if isinstance(ocpt, LeafNode) or ocpt.operator != Operator.XOR:
        return ocpt, False

    relevant_subtrees = [sub for sub in ocpt.subtrees
        if isinstance(sub, OperatorNode) and sub.operator == Operator.XOR and
        all(ot in get_divergent_types(ocpt) for ot in get_divergent_types(sub))]

    if relevant_subtrees:
        return OperatorNode(Operator.XOR, subtrees=[sub for sub in ocpt.subtrees if sub != relevant_subtrees[0]] +
                                                        [sub for sub in relevant_subtrees[0].subtrees]), True

    return ocpt, False



def reduction_rule_three(ocpt):
    if isinstance(ocpt, LeafNode) or ocpt.operator != Operator.SEQUENCE:
        return ocpt, False

    relevant_subtrees = [sub for sub in ocpt.subtrees
        if isinstance(sub, OperatorNode) and sub.operator == Operator.SEQUENCE and
        all(ot in get_divergent_types(sub) for subsub in sub.subtrees for ot in get_divergent_types(subsub))]

    if relevant_subtrees:
        index = ocpt.subtrees.index(relevant_subtrees[0])
        return OperatorNode(Operator.SEQUENCE, subtrees=
        [ocpt.subtrees[i] for i in range(0,index)] + [sub for sub in relevant_subtrees[0].subtrees]
        + [ocpt.subtrees[i] for i in range(index+1,len(ocpt.subtrees))]), True

    return ocpt, False



def reduction_rule_four(ocpt):
    if isinstance(ocpt, LeafNode) or ocpt.operator not in [Operator.SEQUENCE,Operator.XOR,Operator.PARALLEL]:
        return ocpt, False

    relevant_subtrees = [sub for sub in ocpt.subtrees
        if isinstance(sub, OperatorNode) and all(ot in get_divergent_types(OperatorNode(Operator.SEQUENCE,subtrees=[subsub1,subsub2]))
             for subsub1 in sub.subtrees for subsub2 in sub.subtrees for ot in get_related_types(subsub1) & get_related_types(subsub2))]

    if relevant_subtrees:
        index = ocpt.subtrees.index(relevant_subtrees[0])
        return OperatorNode(ocpt.operator, subtrees=
        [ocpt.subtrees[i] for i in range(0,index)] + [OperatorNode(ocpt.operator,ocpt.subtrees[index].subtrees)]
        + [ocpt.subtrees[i] for i in range(index+1,len(ocpt.subtrees))]), True

    return ocpt, False



def check_tree_recursively(ocpt):

    for reduction_rule in [reduction_rule_one,reduction_rule_two,reduction_rule_three,reduction_rule_four]:
        ocpt,check = reduction_rule(ocpt)
        if check:
            return ocpt,True

    if isinstance(ocpt,OperatorNode):
        for i in range(len(ocpt.subtrees)):
            sub,check = check_tree_recursively(ocpt.subtrees[i])
            if check:
                ocpt.subtrees[i] = sub
                return ocpt,True

    return ocpt, False



def create_candidate_set(discovered_tree):

    result = [discovered_tree]
    while check_tree_recursively(result[-1])[1]:
        result.append(check_tree_recursively(result[-1])[0])
    return result