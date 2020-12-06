import pymongo
import time

client = pymongo.MongoClient()
eventPagesCollection = client.wikimedia_history_it.pages
usersCollection = client.wikimedia_user_metrics.users

def reset_users():
    print('Removing all pages_events from users', time.time())
    usersCollection.update_many({}, { '$unset': { 'pages_events': '' } }, upsert=False)
    print('Removed all pages_events from users', time.time())

def add_tot_pages_events():
    print('Getting all non-bot users', time.time())
    users = list(usersCollection.find({ "is_bot": False }).sort('id', pymongo.ASCENDING))
    print('Adding all pages_events to users', time.time())
    for user in users:
        n_pages = eventPagesCollection.count({ 'event_user.id': user.get(id) })
        usersCollection.update_one({ 'id': user.get(id) }, { '$set': { 'pages_events': n_pages } })
    print('Added all pages_events to users', time.time())

add_tot_pages_events()