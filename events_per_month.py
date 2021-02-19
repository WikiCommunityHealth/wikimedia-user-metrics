import pymongo
import time
import json

client = pymongo.MongoClient()
eventsCollectionPages = client.wikimedia_history_it.pages
#eventsCollectionRevisions = client.wikimedia_history_it.revisions


def get_events_pages_per_month():
    print('Starting get_events_pages_per_month', time.time())
    result = [None] * 12

    print('Getting parsed users', time.time())
    monthsInfo = list(eventsCollectionPages.aggregate([
        {
            '$match': {
                'event_user.is_bot_by': []
            }
        },
        {
            '$project': {
                'month': {
                    '$month': '$event_timestamp'
                }
            }
        },
        {
            '$group': {
                '_id': '$month',
                'count': {
                    '$sum': 1
                }
            }
        }
    ]))
    print('Got months info', time.time())

    print('Generating object', time.time())
    for monthData in monthsInfo:
        result[monthData['_id'] - 1] = monthData['count']
    print('Generated object', time.time())

    print('Done get_events_pages_per_month', time.time())
    return result


events_pages_per_month = get_events_pages_per_month()
text = json.dumps(events_pages_per_month)

with open("events_pages_per_month.json", 'w') as outfile:
    outfile.write(text)
