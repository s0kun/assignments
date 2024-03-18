import pymongo

# Connecting to mongod server:
def connect(host = "localhost", port = 27017):
    client = pymongo.MongoClient(host, port)
    print(f"Connected to client on {host} : {port}")
    return client
