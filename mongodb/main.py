from connector import *


# Identifying a database:
client = connect()
db = client["movieFlix"]

# The list of collections in mongoDB database:
tableList = []
pathList = []

import bson
from bson.json_util import loads

def readJSONFile(filename):
    """
    Reads a file and returns a list of its lines.
    Args:
        filename: The path to the file to read.
    Returns:
        A list of strings, where each element is a line from the file.
    """
    with open(filename, 'r') as file:
        lines = file.readlines()
    return [loads(l) for l in lines]

def loadData(ctx):
    """
    Loads the data specified by dataPath into
    the mongoDB collection 'id'
    Args:
        ctx = (id, dataPath)
    Returns:
        None
    """
    id, dataPath = ctx

    docs = readJSONFile(dataPath)
    id.insert_many(docs)

    return

# Defining collections in 'db' and the filepaths:
tableList.append(db["comments"])
pathList.append("sample_mflix/comments.json")

tableList.append(db["movies"])
pathList.append("sample_mflix/movies.json")

tableList.append(db["theatres"])
pathList.append("sample_mflix/theaters.json")

tableList.append(db["users"])
pathList.append("sample_mflix/users.json")

# Total number of collections
size = 4
assert size == len(tableList) and size == len(pathList)

# Bulk load the data
for i in range(size):
    loadData((tableList[i], pathList[i]))
    print(f"Loaded: {pathList[i]}")



