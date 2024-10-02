import sys
import os
import pickle
from pulp import *
import time
import requests
import fire
from collections import defaultdict

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
print(current, parent)
sys.path.append(parent)

import utils


def prepare(path):
    with open(path + "/supply.pkl", 'rb') as file:
        supply_data = pickle.load(file)
    with open(path + "/demand.pkl", 'rb') as file:
        demand_data = pickle.load(file)
    with open(path + "/costs.pkl", 'rb') as file:
        costs_data = pickle.load(file)

    # словарь дорога - образование по периодам
    supply_roads = set([s_data["supply_road"] for s_data in supply_data])
    supply_dict = defaultdict(dict)
    for r in supply_roads:
        supply_dict[r] = {'1': 0, '6': 0}
    for s_data in supply_data:
        supply_dict[s_data['supply_road']][str(s_data['supply_period'])] = s_data["supply_qty"]
    with open(path + "/supply_dict.pkl", 'wb') as file:
        pickle.dump(supply_dict, file, protocol=pickle.HIGHEST_PROTOCOL)

    # словарь дорога - потребность по периодам
    demand_roads = set([d_data["demand_road"] for d_data in demand_data])
    demand_dict = defaultdict(dict)
    for r in demand_roads:
        demand_dict[r] = {'5': 0, '8': 0, '10': 0, '15': 0}
    for d_data in demand_data:
        demand_dict[d_data['demand_road']][str(d_data['demand_period'])] = d_data["demand_qty"]
    with open(path + "/demand_dict.pkl", 'wb') as file:
        pickle.dump(demand_dict, file, protocol=pickle.HIGHEST_PROTOCOL)

    # меняем полный список образования порожняка на сокращенный (за вычетом принудительных назначений)
    with open(path + "/exempted_supply.pkl", 'rb') as file:
        supply_data = pickle.load(file)

    # список узлов образования доступных под погрузку порожних вагонов (дорога + id)
    RailRoads_supply = []
    for road in supply_data:
        if road == 'exempted':
            continue
        RailRoads_supply.append(road['supply_road'] + '_' + str(road['id']))

    # список узлов спроса на вагоны (дорога + id)
    RailRoads_demand = []
    for road in demand_data:
        RailRoads_demand.append(road['demand_road'] + '_' + str(road['id']))

    # создание списка стоимостей всех возможных направлений подсыла порожних вагонов под погрузку
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
                    penalty = utils.penalties(delivery_deviation, s_period)
                    costs_sublist.append(cost['cost'] + penalty)
        costs.append(costs_sublist)

    # стоимости направлений сводим в словарь
    costs = makeDict([RailRoads_supply, RailRoads_demand], costs, 0)

    # создаем экземпляр класса LP задачи для загрузки в него данных
    prob = LpProblem("RailCars Assignment Problem", LpMinimize)

    # создаем лист кортежей со всеми возможными направлениями подсыла порожних вагонов под потребности клиентов
    Routes = [(s, d) for s in RailRoads_supply for d in RailRoads_demand]

    # создаем словарь переменных целевой функции LP задачи
    vars = LpVariable.dicts("Route", (RailRoads_supply, RailRoads_demand), 0, None, LpInteger)

    # задаем целевую функцию и добавляем ее в LP задачу
    prob += (
        lpSum([vars[s][d] * costs[s][d] for (s, d) in Routes]),
        "Sum_of_Transporting_Costs",
    )

    # Создаем перечень ограничений по сумме образующихся вагонов на узлах модели LP и добавляем в модель LP
    total_supply = 0
    for s in RailRoads_supply:
        supply_quantity = supply_data[int(s[4:]) - 1]['supply_qty']
        total_supply += supply_quantity
        prob += (
            lpSum([vars[s][d] for d in RailRoads_demand]) <= supply_quantity,
            f"Sum_of_Products_out_of_Warehouse_{s}",
        )

    # Создаем перечень ограничений по сумме спроса на вагоны на узлах модели LP и добавляем в модель LP
    total_demand = 0
    for d in RailRoads_demand:
        demand_quantity = demand_data[int(d[4:]) - 1]['demand_qty']
        total_demand += demand_quantity
        prob += (
            lpSum([vars[s][d] for s in RailRoads_supply]) >= demand_quantity,
            f"Sum_of_Products_into_Bar{d}",
        )

    with open(path + "/prob.pkl", 'wb') as file:
        pickle.dump(prob.to_dict(), file, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    fire.Fire(prepare)
