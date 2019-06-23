
import pymongo
import json

# connecting to the mongodb
myclient = pymongo.MongoClient('172.18.0.4', 27017)

# creating a database 'mydb' in mongodb
mydb = myclient["mydb"]

# creating a collection 'mycollection' in 'mydb'
mycol = mydb["mycollection"]

#  reading json in python
with open('/home/nishanth/mongodb_python_R/data/laureates.json') as f:
    json_data = json.load(f)

# inserting json data into collection 'mycollection'
mycol.insert(json_data)

# printing the database names
print(myclient.list_database_names())
