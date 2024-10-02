import pandas as pd
import fire
import time
import random
import numpy as np
import requests
import pickle
import sys
from collections import defaultdict


def exempt(path):
    try:
        with open(path + "/supply.pkl", 'rb') as file:
            exempted_supply = pickle.load(file)
        print("Info: supply data loaded from project_pipeline/fetched_data/ folder!")
    except Exception as e:
        print(f"Warning: No supply data loaded! Occurred error: {e}")

    exemptions_rules = {
        'CleanOp': "В промывку на дорогу:",
        'DelayOp': "Затягивание грузовой операции на дороге:",
        'RingOp': "В кольце на дорогу:",
        'SpreadOp': "В распыление на дорогу:",
        'StorageOp': "В отстой на дорогу:",
        'MaintOp': "В ремонт на дорогу:",
        'RentOp': "В аренду на дорогу:"
    }
    exemptions_list_check = [k for k, v in exemptions_rules.items()]

    exemptions = defaultdict(list)
    for idx, s in enumerate(exempted_supply):
        exemption_check = None
        for k, v in s.items():
            if k not in exemptions_list_check:
                continue
            elif k == 'CleanOp' and v == "В промывку":
                exemption_check = (k, v)
            elif k != 'CleanOp' and v != "нет":
                exemption_check = (k, v)

        if exemption_check:
            exemptions['supply_road'].append(s['supply_road'])
            exemptions['supply_period'].append(s['supply_period'])
            exemptions['supply_qty'].append(s['supply_qty'])
            exemptions['demand_road'].append(s['supply_road'])
            exemptions['demand_period'].append(s['supply_period'])
            exemptions['demand_qty'].append(0)
            exemptions['assigned_quantity'].append(s['supply_qty'])
            if exemption_check[0] == 'CleanOp':
                exemptions['assignment_type'].append(f"{exemptions_rules[exemption_check[0]]} {s['supply_road']}")
            else:
                exemptions['assignment_type'].append(f"{exemptions_rules[exemption_check[0]]} {exemption_check[1]}")

            exempted_supply[idx] = 'exempted'

    exemptions_df = pd.DataFrame(exemptions)
    try:
        with open(path + "/exempted_supply.pkl", 'wb') as file:
            pickle.dump(exempted_supply, file, protocol=pickle.HIGHEST_PROTOCOL)
        with open(path + "/exemptions.pkl", 'wb') as file:
            pickle.dump(exemptions_df, file, protocol=pickle.HIGHEST_PROTOCOL)
        print("Info: Exemptions data saved to project_pipeline/fetched_data/ folder!")
    except Exception as e:
        print(f"Warning: Exemptions data not saved! Occurred error: {e}")

    print(exemptions_df)


if __name__ == '__main__':
    fire.Fire(exempt)
