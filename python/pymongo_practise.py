

# %% [markdown]

# ## Loading required packages and creating database and collections

# %%

# importing required packages
import pymongo
import json

# connecting to the mongodb
myclient = pymongo.MongoClient('172.18.0.4', 27017)

# creating a database 'mydb' in mongodb
mydb = myclient["mydb"]

# creating a collection 'mycollection' and 'mycollection1' in 'mydb'
mycol = mydb["mycollection"]

mycol1 = mydb["mycollection1"]

#  reading json in python
with open('./data/laureates.json') as f:
    json_data = json.load(f)

with open('./data/prizes.json') as f:
    json_data1 = json.load(f)

# inserting json data into collection 'mycollection'
mycol.insert(json_data)

# inserting json data into collection 'mycollection1'
mycol1.insert(json_data1)

# printing the database names
print(myclient.list_database_names())


# %% [markdown]

# ## pymongo functions for quering mongodb

# ### find_one() return a document

# %%

# find_one() return a document
print(mydb.mycollection.find_one())

# %% [markdown]

# ## print keys in document fetched


# %%

# fetching a document from mongodb
mycollection_doc = mydb.mycollection.find_one()

# print keys in document fetched
print(list(mycollection_doc.keys()))

# %% [markdown]

# ## find() method Selects documents in a collection or view and returns a cursor to the selected documents

# %%

# returns contents of prizes key in the documents of mycollection
prices_json = [doc['prizes'] for doc in mydb.mycollection.find()]

print(prices_json)


# %%
