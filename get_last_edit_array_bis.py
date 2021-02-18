import pymongo
import time
import json

usersCollection = pymongo.MongoClient()

GAP = 3


def get_gap_in_months(first_month, last_month):
    first_year, first_month = [int(el) for el in first_month.split('/')]
    last_year, last_month = [int(el) for el in last_month.split('/')]
    return (last_year - first_year) * 12 + (last_month - first_month)


def to_keep(last_month, first_month):
    return get_gap_in_months(first_month, last_month) >= 3


def get_obj():
    print('Starting get_obj', time.time())
    obj = {}

    print('Getting parsed users', time.time())
    parsedUsers = list(usersCollection.aggregate([
        {
            '$match': {
                'lastMonth': {
                    '$ne': None
                }
            }
        }, {
            '$project': {
                '_id': False,
                'id': True,
                'lastMonth': True,
                'firstMonth': True
            }
        }
    ]))
    print('Got parsed users ' + str(len(parsedUsers)), time.time())

    print('Generating object', time.time())
    for parsedUser in parsedUsers:
        last_month = parsedUser['lastMonth']
        first_month = parsedUser['firstMonth']
        if (to_keep(last_month, first_month)):
            if last_month not in obj:
                obj[last_month] = 0
            else:
                obj[last_month] += 1
    print('Generated object', time.time())

    print('Done get_obj', time.time())
    return obj


obj = get_obj()
text = json.dumps(obj)

with open("last_edit_object_more_than_months_" + str(GAP) + ".json", 'w') as outfile:
    outfile.write(text)
