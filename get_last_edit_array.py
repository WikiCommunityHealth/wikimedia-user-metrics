import pymongo
import time
import json

client = pymongo.MongoClient()
usersCollection = client.wikimedia_user_metrics.users

def get_months():
    months = []
    for year in range(2002, 2021):
        for month in (range(1, 13) if year < 2020 else range(1, 10)):
            months.append(str(year) + "/" + str(month))
    return months

def get_obj():
    print('Starting get_obj', time.time())
    months = get_months()
    obj = {}

    for month in months:
        print('Doing month', month, time.time())
        obj[month] = usersCollection.count_documents({ 'lastMonth': month })
    
    print('Done get_obj', time.time())
    return obj


obj = get_obj()
text = json.dumps(obj)

with open("last_edit_object.json", 'w') as outfile:
    outfile.write(text)


    