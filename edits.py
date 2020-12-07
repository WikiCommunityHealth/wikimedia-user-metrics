import pymongo
import time
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
eventRevisionsCollection = client.wikimedia_history_it.revisions
usersCollection = client.wikimedia_user_metrics.users

print('Getting all non-bot users ids', time.time())
idsResult = list(usersCollection.aggregate([
    {
        '$sort': { 'id': 1 }
    },
    {
        '$limit': 100
    },
    {
        '$match': {
            'is_bot': False
        }
    },
    {
        '$group': {
            '_id': None,
            'ids': {
                '$push': '$id'
            }
        }
    }
]))
ids = idsResult[0].get('ids')
print('Getted all non-bot users ids', time.time())

print('Getting all data from revisions', time.time())
results = list(eventRevisionsCollection.aggregate([
    {
        '$match': {
            'event_user.id': {
                '$in': ids
            }
        }
    }, {
        '$group': {
            '_id': {
                'id': '$event_user.id', 
                'y': {
                    '$year': '$event_timestamp'
                }, 
                'm': {
                    '$month': '$event_timestamp'
                }
            }, 
            'n': {
                '$sum': 1
            }
        }
    }, {
        '$project': {
            '_id': False, 
            'id': '$_id.id', 
            'v': [
                [
                    {
                        '$concat': [
                            {
                                '$toString': '$_id.y'
                            }, '/', {
                                '$toString': '$_id.m'
                            }
                        ]
                    }, '$n'
                ]
            ]
        }
    }, {
        '$project': {
            'id': 1, 
            'v': {
                '$arrayToObject': '$v'
            }
        }
    }, {
        '$group': {
            '_id': '$id', 
            'edits': {
                '$mergeObjects': '$v'
            }
        }
    }, {
        '$project': {
            '_id': False,
            'f': {
                'id': '$_id'
            }, 
            'u': {
                'set': {
                    'edits': '$edits'
                }
            }
        }
    }
]))
print('Getted all data from revisions', time.time())

print('Converting all data to update queries', time.time())
for r in results:
    r['u']['$set'] = r['u'].pop('set')
print('Converted all data to update queries', time.time())

print('Converting all data to update queries bis', time.time())
results = [pymongo.UpdateOne(r['f'], r['u']) for r in results]
print('Converted all data to update queries bis', time.time())

print('Updating all user documents', time.time())
usersCollection.bulk_write(results)
print('Updated all user documents', time.time())