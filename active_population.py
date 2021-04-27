import json

def get_months():
    months = []
    for year in range(2001, 2021):
        for month in (range(1, 13) if year < 2020 else range(1, 10)):
            months.append(str(year) + "/" + str(month))
    return months

def get_progressive_last_edits(last_edits):
    months = get_months()
    last = 0
    result = {}
    for month in months:
        result[month] = last + (last_edits[month] if month in last_edits else 0)
        last = result[month]
    return result


with open('last_edit_object.json') as last_edit_txt:
    last_edit_txt = last_edit_txt.read()
    with open('population_history.json') as population_txt:
        population_txt = population_txt.read()
        last_edit = get_progressive_last_edits(json.loads(last_edit_txt))
        population = json.loads(population_txt)
        obj = {}
        for (month, n_users) in list(last_edit.items())[40:]:
            obj[month] = population[month] - n_users
        with open('population_history_active.json', 'w') as out:
            out.write(json.dumps(obj, sort_keys=True))
