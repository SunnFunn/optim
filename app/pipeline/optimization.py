import sys, os
import pandas as pd
from pulp import *
import time
import random
import numpy as np
import requests
from collections import defaultdict

from utils import penalties
from exemptions import exempt
from OPZ_format import output_format


base_dir = os.path.abspath(os.path.dirname(__file__)) + '/output_data/'
output_path = base_dir + 'OPZ.xlsx'

supply_url = "http://127.0.0.1:8000/supply"
demand_url = "http://127.0.0.1:8000/demand"
costs_url = "http://127.0.0.1:8000/costs"

try:
    supply_response = requests.get(supply_url)
    demand_response = requests.get(demand_url)
    costs_response = requests.get(costs_url)

    supply_data = supply_response.json()
    demand_data = demand_response.json()
    costs_data = costs_response.json()
except:
    print("Warning: API not responding, no data received!")
    sys.exit()

supply_roads = set([s["supply_road"] for s in supply_data if s != "exempted"])
supply_dict = defaultdict(dict)
for r in supply_roads:
    supply_dict[r] = {'1': 0, '6': 0}
for s in supply_data:
    if s != "exempted":
        supply_dict[s['supply_road']][f"{s['supply_period']}"] = s["supply_qty"]

demand_roads = set([d["demand_road"] for d in demand_data])
demand_dict = defaultdict(dict)
for r in demand_roads:
    demand_dict[r] = {'5': 0, '8': 0, '10': 0, '15': 0}
for d in demand_data:
    demand_dict[d['demand_road']][f"{d['demand_period']}"] = d["demand_qty"]

exemption_df, supply_data = exempt(supply_data)

RailRoads_supply = []
for road in supply_data:
    if road == 'exempted':
        continue
    RailRoads_supply.append(road['supply_road'] + '_' + str(road['id']))

RailRoads_demand = []
for road in demand_data:
    RailRoads_demand.append(road['demand_road'] + '_' + str(road['id']))

# Creates a list of costs of each transportation path
costs = []
for s in RailRoads_supply:
    costs_sublist = []
    for d in RailRoads_demand:
        s_road = s[:3]
        d_road = d[:3]
        s_period = supply_data[int(s[4:]) - 1]['supply_period']
        d_period = demand_data[int(d[4:]) - 1]['demand_period']
        for cost in costs_data:
            if cost['supply_road'] == s_road and cost['demand_road'] == d_road:
                delivery_deviation = abs(int(cost['cost']) - (int(d_period) - int(s_period)))
                penalty = penalties(delivery_deviation, s_period)
                costs_sublist.append(cost['cost'] + penalty)
    costs.append(costs_sublist)

# The cost data is made into a dictionary
costs = makeDict([RailRoads_supply, RailRoads_demand], costs, 0)

# Creates the 'prob' variable to contain the problem data
prob = LpProblem("RailCars Assignment Problem", LpMinimize)

# Creates a list of tuples containing all the possible routes for transport
Routes = [(s, d) for s in RailRoads_supply for d in RailRoads_demand]

# A dictionary called 'Vars' is created to contain the referenced variables(the routes)
vars = LpVariable.dicts("Route", (RailRoads_supply, RailRoads_demand), 0, None, LpInteger)

# The objective function is added to 'prob' first
prob += (
    lpSum([vars[s][d] * costs[s][d] for (s, d) in Routes]),
    "Sum_of_Transporting_Costs",
)

# The supply maximum constraints are added to prob for each supply node (warehouse)
total_supply = 0
for s in RailRoads_supply:
    supply_quantity = supply_data[int(s[4:]) - 1]['supply_qty']
    total_supply += supply_quantity
    prob += (
        lpSum([vars[s][d] for d in RailRoads_demand]) <= supply_quantity,
        f"Sum_of_Products_out_of_Warehouse_{s}",
    )

# The demand minimum constraints are added to prob for each demand node (bar)
total_demand = 0
for d in RailRoads_demand:
    demand_quantity = demand_data[int(d[4:]) - 1]['demand_qty']
    total_demand += demand_quantity
    prob += (
        lpSum([vars[s][d] for s in RailRoads_supply]) >= demand_quantity,
        f"Sum_of_Products_into_Bar{d}",
    )


def optimize(p, exemptions):
    # The problem data is written to an .lp file
    #p.writeLP("C:/Users/Алексей Третьяков/Desktop/PuLPRail/TransportLP.lp")

    # The problem is solved using PuLP's choice of Solver
    solver = PULP_CBC_CMD(msg=False)
    # solver.tmpDir = "C:/Users/Алексей Третьяков/Desktop/TEMP/"
    p.solve(solver)

    # The status of the solution is printed to the screen
    print("Status:", LpStatus[p.status])

    # Each of the variables is printed with it's resolved optimum value

    assignments = defaultdict(list)
    total_assignments = 0
    for v in p.variables():
        if v.varValue > 0:
            v_split = v.name.split('_')
            supply_period = supply_data[int(v_split[2]) - 1]['supply_period']
            #print(v_split)
            #print(supply_data[int(v_split[2]) - 1])
            assignment_period = demand_data[int(v_split[4]) - 1]['demand_period']
            if supply_period == 1:
                assignments['supply_road'].append(v_split[1])
                assignments['supply_period1'].append(supply_dict[v_split[1]]['1'])
                assignments['supply_period10'].append(supply_dict[v_split[1]]['6'])
                if demand_dict[v_split[1]] == {}:
                    assignments['demand_period5'].append(0)
                    assignments['demand_period8'].append(0)
                    assignments['demand_period10'].append(0)
                    assignments['demand_period15'].append(0)
                else:
                    assignments['demand_period5'].append(demand_dict[v_split[1]]['5'])
                    assignments['demand_period8'].append(demand_dict[v_split[1]]['8'])
                    assignments['demand_period10'].append(demand_dict[v_split[1]]['10'])
                    assignments['demand_period15'].append(demand_dict[v_split[1]]['15'])

                assignments['assignment_road'].append(v_split[3])
                assignments['assigned_quantity'].append(int(v.varValue))
                assignments['assignment_type'].append(f"Под погрузку в {assignment_period} сутки")

        total_assignments += v.varValue
    output_df = pd.DataFrame(assignments)
    print("Total Supply = ", int(total_supply))
    print("Total Demand = ", int(total_demand))
    print("Total Assignments = ", int(total_assignments))

    # The optimised objective function value is printed to the screen
    print("Total Cost of Transportation = ", int(value(p.objective)))

    for _, row in exemptions.iterrows():
        supply_road = row.supply_road
        demand_road = row.demand_road
        demand5 = 0 if demand_dict[demand_road] == {} else demand_dict[demand_road]['5']
        demand8 = 0 if demand_dict[demand_road] == {} else demand_dict[demand_road]['8']
        demand10 = 0 if demand_dict[demand_road] == {} else demand_dict[demand_road]['10']
        demand15 = 0 if demand_dict[demand_road] == {} else demand_dict[demand_road]['15']
        output_df.loc[len(output_df.index)] = [supply_road,
                                               supply_dict[supply_road]['1'],
                                               supply_dict[supply_road]['6'],
                                               demand5,
                                               demand8,
                                               demand10,
                                               demand15,
                                               demand_road,
                                               row.assigned_quantity,
                                               row.assignment_type]

    output_df = output_df.sort_values(by=['supply_road'])

    return output_df


if __name__ == '__main__':
    start = time.time()
    output_df = optimize(prob, exemption_df)
    output_format(output_df, output_path)
    end = time.time()

    print(f"total compute time: {round((end - start), 2)} секунд")

