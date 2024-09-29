import pandas as pd
from pulp import *
import time
import random
import numpy as np
import requests
import sys
from collections import defaultdict


def exempt(supply):
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
    for idx, s in enumerate(supply):
        exemptions_check = None
        for k, v in s.items():
            if k not in exemptions_list_check:
                continue
            elif k == 'CleanOp' and v == "В промывку":
                exemptions_check = (k, v)
            elif k != 'CleanOp' and v != "нет":
                exemptions_check = (k, v)

        if exemptions_check:
            exemptions['supply_road'].append(s['supply_road'])
            exemptions['supply_period'].append(s['supply_period'])
            exemptions['supply_qty'].append(s['supply_qty'])
            exemptions['demand_road'].append(s['supply_road'])
            exemptions['demand_period'].append(s['supply_period'])
            exemptions['demand_qty'].append(0)
            exemptions['assigned_quantity'].append(s['supply_qty'])
            if exemptions_check[0] == 'CleanOp':
                exemptions['assignment_type'].append(f"{exemptions_rules[exemptions_check[0]]} {s['supply_road']}")
            else:
                exemptions['assignment_type'].append(f"{exemptions_rules[exemptions_check[0]]} {exemptions_check[1]}")

            supply[idx] = 'exempted'

    exemptions_df = pd.DataFrame(exemptions)

    return exemptions_df, supply
