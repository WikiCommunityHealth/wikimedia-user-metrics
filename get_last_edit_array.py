import pymongo
import time
import json

from sshtunnel import SSHTunnelForwarder

def get_mongo_client():
    server = SSHTunnelForwarder(
        ('192.168.184.92',22),
        ssh_username='berretta',
        ssh_password='Fuck,eugenio3361!?',
        remote_bind_address=('127.0.0.1', 27017)
    )

    server.start()
    client = pymongo.MongoClient(host='127.0.0.1', port=server.local_bind_port)
    return client

client = get_mongo_client()
usersCollection = client.wikimedia_user_metrics.users

THRESHOLD = 10


def get_months():
    months = []
    for year in range(2002, 2021):
        for month in (range(1, 13) if year < 2020 else range(1, 10)):
            months.append(str(year) + "/" + str(month))
    return months


def get_obj():
    print('Starting get_obj', time.time())
    obj = {}

    print('Getting parsed users', time.time())
    parsedUsers = list(usersCollection.aggregate([
        {
            '$match': {
                'is_bot': False,
                'lastMonth': { '$ne': None }
            }
        }, {
            '$project': {
                'id': True,
                'lastMonth': True,
                'events_tot': {
                    '$sum': [
                        '$n_pages_created', '$n_pages_deleted', '$n_pages_edited', '$n_pages_moved', '$n_pages_restored'
                    ]
                }
            }
        }, {
            '$match': {
                'events_tot': {
                    '$gte': THRESHOLD
                }
            }
        }
    ]))
    print(f'Got parsed users {len(parsedUsers)}', time.time())

    print('Generating object', time.time())
    for parsedUser in parsedUsers:
        obj[parsedUser['lastMonth']] += 1
    print('Generated object', time.time())

    print('Done get_obj', time.time())
    return obj


obj = get_obj()
text = json.dumps(obj)

with open(f"last_edit_object_more_than_{THRESHOLD}.json", 'w') as outfile:
    outfile.write(text)
