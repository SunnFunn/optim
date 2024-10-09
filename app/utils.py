import pandas as pd
import numpy as np


def penalties(time_deviation, supply_period):
    penalty = time_deviation**2 if supply_period == 1 else 2000
    return int(penalty)


def output_format(df, path):
    roads = [""]
    for idx, row in df.iterrows():
        roads.append(row.supply_road)
        if roads[-1] == roads[-2]:
            df.loc[idx] = [np.nan]*7 + [row.assignment_road, row.assigned_quantity, row.assignment_type]
    with pd.ExcelWriter(path) as writer:
        df.to_excel(writer, engine='xlsxwriter', sheet_name='OPZ', index=False)

        sheet = writer.sheets['OPZ']
        sheet.set_default_row(20)
        general_format = writer.book.add_format({'num_format': '0',
                                                 'align': 'center'})
        sheet.set_column(0, 8, 12, general_format)
        sheet.set_column(9, 9, 50, general_format)
        #sheet.set_default_row(20)
        #sheet.set_column(0, 0, 5)
        #sheet.set_column(1, 1, 55)
        #sheet.set_column(2, 6, 25)