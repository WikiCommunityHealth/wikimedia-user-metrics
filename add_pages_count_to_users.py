import pymongo
from dateutil.rrule import rrule, MONTHLY
from datetime import datetime
import time

client = pymongo.MongoClient()
eventPagesCollection = client.wikimedia_history_it.pages
eventRevisionsCollection = client.wikimedia_history_it.revisions
usersCollection = client.wikimedia_user_metrics.users

def reset_users():
    print('Removing all pages_events from users', time.time())
    usersCollection.update_many({}, { '$unset': { 'pages_events': '' } }, upsert=False)
    print('Removed all pages_events from users', time.time())

def n_pages_by_type_and_user(id, event_type):
    return eventPagesCollection.count({ 'event_user.id': id, 'event_type': event_type })

def n_revisions_by_user(id):
    return eventRevisionsCollection.count({ 'event_user.id': id })

def n_revisions_by_user_and_month(id, month):
    start, end = month
    start_month, start_year = start
    end_month, end_year = end
    return eventRevisionsCollection.count({ 
        'event_user.id': id, 
        'event_timestamp': { 
            '$gte': datetime(start_year, start_month, 1), 
            '$lt': datetime(end_year, end_month, 1) 
        } 
    })

def add_tot_pages_events():
    print('Getting all non-bot users', time.time())
    users = list(usersCollection.find({ "is_bot": False }).sort('id', pymongo.ASCENDING))
    print('Adding all pages_events to users', time.time())
    for user in users:
        update_object = dict(
            n_pages_created = n_pages_by_type_and_user(user.get('id'), 'create'),
            n_pages_deleted = n_pages_by_type_and_user(user.get('id'), 'delete'),
            n_pages_moved = n_pages_by_type_and_user(user.get('id'), 'move'),
            n_pages_restored = n_pages_by_type_and_user(user.get('id'), 'restore')
        )
        usersCollection.update_one({ 'id': user.get('id') }, { '$set': update_object })
    print('Added all pages_events to users', time.time())

def add_tot_revisions_events():
    print('Getting all non-bot users', time.time())
    users = list(usersCollection.find({ "is_bot": False }).sort('id', pymongo.ASCENDING))
    print('Adding all revisions_events to users', time.time())
    for user in users:
        update_object = dict(n_pages_edited = n_revisions_by_user(user.get('id')))
        usersCollection.update_one({ 'id': user.get('id') }, { '$set': update_object })
    print('Added all revisions_events to users', time.time())

def months_range(start_month, start_year, end_month, end_year):
    start = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)
    months = [(d.month, d.year) for d in rrule(MONTHLY, dtstart=start, until=end)]
    return [(months[i], months[i + 1]) for i in range(0, len(months) - 1)]

def add_month_revisions_events():
    months = months_range(1, 2001, 12, 2020)
    print('Getting all non-bot users', time.time())
    users = list(usersCollection.find({ "is_bot": False }).sort('id', pymongo.ASCENDING).limit(5))
    print('Adding all revisions_events to users', time.time())
    for user in users:
        update_object = { 'events': { } }
        for month in months:
            update_object['events'][f'{month[0][1]}/{month[0][0]}-{month[1][1]}/{month[1][0]}'] = { 'edits': n_revisions_by_user_and_month(user.get('id'), month) }
        usersCollection.update_one({ 'id': user.get('id') }, { '$set': update_object })
    print('Added all revisions_events to users', time.time())

#add_tot_pages_events()
#add_tot_revisions_events()
add_month_revisions_events()