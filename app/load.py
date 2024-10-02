import pickle
import pandas as pd
from collections import defaultdict
from utils import output_format


data_path = "/home/alext/alex/railoptim/data"
output_path = "./output_data/"

with open(data_path + "/exemptions.pkl", 'rb') as file:
    data = pickle.load(file)

#output_format(data, output_path)
data.to_excel(output_path + "exemptions.xlsx", index=False)


if __name__ == '__main__':
    print(data)