import sys
import fire
import requests
import pickle


def fetch_data(url: str, path: str):
    supply_data_url = url + "/supply"
    demand_data_url = url + "/demand"
    costs_data_url = url + "/costs"
    try:
        supply_response = requests.get(supply_data_url)
        demand_response = requests.get(demand_data_url)
        costs_response = requests.get(costs_data_url)

        supply_data = supply_response.json()
        demand_data = demand_response.json()
        costs_data = costs_response.json()
        print("Info: all data received!")

        with open(path + "/supply.pkl", 'wb') as file:
            pickle.dump(supply_data, file, protocol=pickle.HIGHEST_PROTOCOL)
        with open(path + "/demand.pkl", 'wb') as file:
            pickle.dump(demand_data, file, protocol=pickle.HIGHEST_PROTOCOL)
        with open(path + "/costs.pkl", 'wb') as file:
            pickle.dump(costs_data, file, protocol=pickle.HIGHEST_PROTOCOL)
        print("Info: all data saved in project_pipeline/fetched_data/ folder!")
    except Exception as e:
        print(f"Warning: API not responding or data failed to be saved! Occurred error: {e}")
        sys.exit()


if __name__ == '__main__':
    fire.Fire(fetch_data)
