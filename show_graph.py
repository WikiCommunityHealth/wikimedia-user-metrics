import matplotlib.pyplot as plt
import numpy as np
import json
import datetime

TOT_USERS_NON_BOT = 2023922
TOT_USERS_NO_EDIT = 1499720
TOT_USERS_WITH_EDIT = TOT_USERS_NON_BOT - TOT_USERS_NO_EDIT

CURRENT_MONTH = "2020/9"

INPUT_FILE = 'last_edit_object_more_than_10.json'

def get_months():
    months = []
    for year in range(2002, 2021):
        for month in (range(1, 13) if year < 2020 else range(1, 10)):
            months.append(datetime.datetime(year, month, 1))
    return months

def get_gap_in_months(last_month):
    current_year, current_month = [int(el) for el in CURRENT_MONTH.split('/')]
    last_year, last_month = [int(el) for el in last_month.split('/')]
    return (current_year - last_year) * 12 + (current_month - last_month)

def get_months_data():
    with open(INPUT_FILE) as file_in:
        data = file_in.read()
        obj = json.loads(data)
    return obj

def get_population_data():
    with open('population_history.json') as file_in:
        data = file_in.read()
        obj = json.loads(data)
    return obj

obj = get_months_data()
population = get_population_data()
months = get_months()
data = [0] * len(months)
for (key, value) in obj.items():
    data[get_gap_in_months(key)] = (value / population[key] if key in population else 1) * 100
    # data[get_gap_in_months(key)] = (value / TOT_USERS_WITH_EDIT) * 100
    # data[get_gap_in_months(key)] = value

months = months[20:]
months.reverse()

plt.plot(months, data[20:])

plt.show()
