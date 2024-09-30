def penalties(time_deviation, supply_period):
    penalty = time_deviation**2 if supply_period == 1 else time_deviation**4
    return int(penalty)