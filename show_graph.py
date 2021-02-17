import matplotlib.pyplot as plt
import numpy as np
import json

TOT_USERS_NON_BOT = 2023922
TOT_USERS_NO_EDIT = 1501542
TOT_USERS_WITH_EDIT = TOT_USERS_NON_BOT - TOT_USERS_NO_EDIT

CURRENT_MONTH = "2020/9"

def get_gap_in_months(last_month):
    current_year, current_month = [int(el) for el in CURRENT_MONTH.split('/')]
    last_year, last_month = [int(el) for el in last_month.split('/')]
    return (current_year - last_year) * 12 + (current_month - last_month)

def get_months_data():
    with open('last_edit_object.json') as file_in:
        data = file_in.read()
        obj = json.loads(data)
    return obj

obj = get_months_data()
data = [0] * 300
for (key, value) in obj.items():
    data[get_gap_in_months(key)] = value

plt.plot(list(range(0, 300)), data)
plt.show()