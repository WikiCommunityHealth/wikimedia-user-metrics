import pymongo
import time
import json

client = pymongo.MongoClient()
usersCollection = client.wikimedia_user_metrics.users


def get_months():
    months = []
    for year in range(2001, 2021):
        for month in (range(1, 13) if year < 2020 else range(1, 10)):
            months.append(str(year) + "/" + str(month))
    return months


def get_obj():
    print('Starting get_obj', time.time())

    print('Getting data', time.time())
    parsedMonths = list(usersCollection.aggregate([
        {
            '$match': {
                'is_bot': False
            }
        }, {
            '$project': {
                'creation_month': {
                    '$concat': [
                        {
                            '$toString': {
                                '$year': '$created'
                            }
                        }, '/', {
                            '$toString': {
                                '$month': '$created'
                            }
                        }
                    ]
                }
            }
        }, {
            '$group': {
                '_id': '$creation_month',
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$group': {
                '_id': None,
                'data': {
                    '$addToSet': {
                        'k': '$_id',
                        'v': '$count'
                    }
                }
            }
        }, {
            '$project': {
                '_id': False,
                'data': {
                    '$arrayToObject': '$data'
                }
            }
        }
    ]))
    print('Got data ', time.time())

    return parsedMonths[0]['data']


def accumulate(obj):
    months = get_months()
    for i in range(1, len(months)):
        if months[i] in obj:
            prev = obj[months[i - 1]] if months[i - 1] in obj else 0
            obj[months[i]] = obj[months[i]] + prev
    del obj['2020/10']
    del obj['2020/11']
    return obj

obj = get_obj()
obj = accumulate(obj)
text = json.dumps(obj)

with open("population_history.json", 'w') as outfile:
    outfile.write(text)
