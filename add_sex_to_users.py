import pymongo
from dateutil.rrule import rrule, MONTHLY
from datetime import datetime
import time

client = pymongo.MongoClient()
usersCollection = client.wikimedia_user_metrics.users

def update_sex_of_users():
    print('Starting updating the sex of the users', time.time())
    with open('genders-it.tsv') as f:
        for line in f:
            parts = line.split('\t')
            id = int(parts[0]); _sex = parts[2]
            sex = True if _sex == 'male' else (False if _sex == 'female' else None)
            usersCollection.update_one({ "id": id }, { "$set": { "sex": sex } })
    
    print('Finished updating the sex of the users', time.time())

update_sex_of_users()