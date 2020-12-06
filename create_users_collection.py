import pymongo

client = pymongo.MongoClient()
eventUsersCollection = client.wikimedia_history_it.users
usersCollection = client.wikimedia_user_metrics.users

print('Getting sorted and projected users')
users = list(eventUsersCollection.aggregate([
    {
        '$sort': {
            'user.id': 1
        }
    }, {
        '$match': {
            'event_type': 'create'
        }
    }, {
        '$project': {
            'id': '$user.id', 
            'username': '$user.text', 
            'created': '$event_timestamp', 
            'is_bot': {
                '$ne': [
                    '$user.is_bot_by', []
                ]
            }
        }
    }
]))

print('Inserting parsed users')
usersCollection.insert(users)
print('Finished')
