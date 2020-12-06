import pymongo
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

#add_tot_pages_events()
add_tot_revisions_events()