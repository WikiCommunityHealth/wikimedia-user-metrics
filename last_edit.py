import pymongo
import time

client = pymongo.MongoClient()
usersCollection = client.wikimedia_user_metrics.users

def add_last_edit():
    print('Starting add_last_edit', time.time())
    print('Getting all data from revisions', time.time())
    results = list(usersCollection.aggregate([
        {
            '$match': {
                'is_bot': False
            }
        },
        {
            '$project': {
                'id': True,
                'editsArray': {
                    '$objectToArray': '$events.edit.months'
                },
                'deleteArray': {
                    '$objectToArray': '$events.delete.months'
                },
                'createArray': {
                    '$objectToArray': '$events.create.months'
                },
                'moveArray': {
                    '$objectToArray': '$events.move.months'
                },
                'restoreArray': {
                    '$objectToArray': '$events.restore.months'
                }
            }
        }, {
            '$project': {
                'id': True,
                'eventDates': {
                    '$concatArrays': [
                        '$editsArray', '$deleteArray', '$createArray', '$moveArray', '$restoreArray'
                    ]
                }
            }
        }, {
            '$project': {
                'id': True,
                'eventDates': {
                    '$map': {
                        'input': '$eventDates',
                        'as': 'el',
                        'in': '$$el.k'
                    }
                }
            }
        }, {
            '$project': {
                'id': True,
                'lastMonth': {
                    '$max': '$eventDates'
                }
            }
        }, {
            '$project': {
                '_id': False,
                'f': {
                    'id': '$id'
                },
                'u': {
                    'set': {
                        'lastMonth': '$lastMonth'
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
    print('Getted', len(results), 'results')
    print('Converted all data to update queries bis', time.time())

    print('Updating all user documents', time.time())
    usersCollection.bulk_write(results)
    print('Updated all user documents', time.time())
    print('Ending add_edits', time.time())


add_last_edit()
