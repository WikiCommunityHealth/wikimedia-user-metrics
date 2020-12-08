import pymongo
import time

client = pymongo.MongoClient()
eventPagesCollection = client.wikimedia_history_it.pages
usersCollection = client.wikimedia_user_metrics.users

PAGE_SIZE = 200000

def add_page_events():
    print('Starting add_page_events', time.time())
    for i in range(0, 20):
        print('Getting all non-bot users ids', i, time.time())
        idsResult = list(usersCollection.aggregate([
            {
                '$match': {
                    'is_bot': False
                }
            },
            {
                '$sort': { 'id': 1 }
            },
            {
                '$skip': i * PAGE_SIZE
            },
            {
                '$limit': PAGE_SIZE
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
        if (len(idsResult) < 1):
            break
        ids = idsResult[0]['ids']
        print('Getted ', len(ids), 'ids')
        print('Getted all non-bot users ids', time.time())

        print('Getting all data from pages', time.time())
        results = list(eventPagesCollection.aggregate([
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
                        'type': '$event_type', 
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
                    'type': '$_id.type', 
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
                    'type': 1, 
                    'v': {
                        '$arrayToObject': '$v'
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'id': '$id', 
                        'type': '$type'
                    }, 
                    'events': {
                        '$mergeObjects': '$v'
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
                                    'events', '.', '$_id.type', '.', 'months'
                                ]
                            }, '$events'
                        ]
                    ]
                }
            }, {
                '$project': {
                    'f': {
                        'id': '$id'
                    }, 
                    'u': {
                        'set': {
                            '$arrayToObject': '$v'
                        }
                    }
                }
            } 
        ]))
        print('Getted all data from pages', time.time())

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
    print('Ending add_page_events', time.time())

def reset_page_events():
    print('Starting reset_page_events', time.time())
    usersCollection.update_many({ 'is_bot': False }, { '$set': { 
        'events.create.months': {}, 
        'events.move.months': {}, 
        'events.delete.months': {}, 
        'events.restore.months': {} 
    }})
    print('End reset_page_events', time.time())

reset_page_events()
add_page_events()