import pymongo
import time

client = pymongo.MongoClient()
eventPagesCollection = client.wikimedia_history_it.pages
usersCollection = client.wikimedia_user_metrics.users

print('Getting all non-bot users', time.time())
users = list(usersCollection.find({ "is_bot": False }).sort('id', pymongo.ASCENDING))

print('Starting everything', time.time())
for user in users:
    n_pages = list(eventPagesCollection.count({ 'event_user.id': user.id }))
    usersCollection.update_one({ 'id': user.id }, { '$set': { 'pages_events': n_pages } })
print('Finished', time.time())
