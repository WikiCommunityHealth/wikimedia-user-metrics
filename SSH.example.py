import pymongo
from sshtunnel import SSHTunnelForwarder

def get_mongo_client():
    server = SSHTunnelForwarder(
        ('192.168.184.92',22),
        ssh_username='<USER>',
        ssh_password='<PWD>',
        remote_bind_address=('127.0.0.1', 27017)
    )

    server.start()
    client = pymongo.MongoClient(host='127.0.0.1', port=server.local_bind_port)
    return client