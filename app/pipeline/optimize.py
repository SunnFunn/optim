import sys
import os
import pickle
import fire
import pandas as pd
from pulp import *
from time import *
import requests
from collections import defaultdict

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

import utils


def optimize(path):
    local_time = strftime("%Y-%m-%d-%H-%M", localtime())
    start = time()
    with open(path + "/prob.pkl", 'rb') as file:
        p_data = pickle.load(file)
    with open(path + "/exempted_supply.pkl", 'rb') as file:
        supply_data = pickle.load(file)
    with open(path + "/demand.pkl", 'rb') as file:
        demand_data = pickle.load(file)
    with open(path + "/supply_dict.pkl", 'rb') as file:
        supply_dict = pickle.load(file)
    with open(path + "/demand_dict.pkl", 'rb') as file:
        demand_dict = pickle.load(file)

    _, p = LpProblem.from_dict(p_data)

    # Данные с описанием LP задачи записываются ва файл с расширением .lp
    p.writeLP(path + f"/optim_{local_time}.lp")

    # Задаем родной солвер и запускаем рещение задачи LP
    solver = PULP_CBC_CMD(msg=False)
    # solver.tmpDir = "C:/Users/Алексей Третьяков/Desktop/TEMP/"
    p.solve(solver)

    # Статус решения потом будет записан в короткий итоговый текстовый файл
    solver_status = LpStatus[p.status]

    # запишем решение для каждой переменной (кол-во назначенных на направлении вагонов) в словарь
    assignments = defaultdict(list)
    total_assignments = 0
    for v in p.variables():
        if v.varValue > 0:
            v_split = v.name.split('_')
            supply_period = supply_data[int(v_split[2]) - 1]['supply_period']
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
    end = time()

    output_df = pd.DataFrame(assignments)
    with open(path + "/exemptions.pkl", 'rb') as file:
        exemptions = pickle.load(file)

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
                                               row.assignment_road,
                                               row.assigned_quantity,
                                               row.assignment_type]

    utils.output_format(output_df.sort_values(by=['supply_road']), path + "/OPZ.xlsx")

    with open(path + "/supply.pkl", 'rb') as file:
        full_supply_data = pickle.load(file)
    total_supply = 0
    for supply in full_supply_data:
        total_supply += supply['supply_qty']
    total_demand = 0
    for demand in demand_data:
        total_demand += demand['demand_qty']

    with open(path + f"/optim_status_{local_time}.txt", "w") as status_file:
        status_file.write(f"Status:\n{solver_status}\nTotal Supply = {int(total_supply)}\n"
                          f"Total Demand = {int(total_demand)}\nTotal Assignments = {int(total_assignments)}\n"
                          f"Total Cost of Transportation =  {int(value(p.objective))}\n"
                          f"Total compute time: {round((end - start), 2)} секунд")


if __name__ == '__main__':
    fire.Fire(optimize)

