

from src_journal import extended_df2_miner_apply






#case study for the showcase application --> order management model with synchronizations
blocked = ["subset_sync", "implication"]
eocpt = extended_df2_miner_apply("data/01_ocel_standard_order_management.json",
                        0.90,1.00,blocked)
print(str(eocpt))
exit()


#case study for the showcase application --> order management model with implications
blocked = ["subset_sync", "strict_sync"]
eocpt = extended_df2_miner_apply("data/01_ocel_standard_order_management.json",
                        0.90,1.00,blocked)
print(str(eocpt))





#case study for the showcase application --> recruitment model with synchronizations
blocked = ["subset_sync", "implication"]
eocpt = extended_df2_miner_apply("data/10_ocel_legacy_recruiting.jsonocel",
                        0.90,0.7,blocked)
print(str(eocpt))


#case study for the showcase application --> recruitment model with implications
blocked = ["subset_sync", "strict_sync"]
eocpt = extended_df2_miner_apply("data/10_ocel_legacy_recruiting.jsonocel",
                        0.90,1.00,blocked)
print(str(eocpt))


#case study for the showcase application --> recruitment model with implications
blocked = ["subset_sync", "strict_sync"]
eocpt = extended_df2_miner_apply("data/10_ocel_legacy_recruiting.jsonocel",
                        0.90,0.90,blocked)
print(str(eocpt))
