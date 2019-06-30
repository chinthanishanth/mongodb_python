

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


# %% [markdown]

# ## operators in mongodb
#  Following are the few query selection operators in mongodb <br>
#  for more details check the following documentation<br>
#  https: // docs.mongodb.com/manual/reference/operator/query/

# %% [markdown]

# #### equals($eq), lessthan($lt)

# %%

# lessthan ($lt)
count = mydb.mycollection.count_documents({"born": {"$lt": "1900"}})
print(count)
# equals ($eq)
count = mydb.mycollection.count_documents({"firstname": {"$eq": "Marie"}})
print(count)
# or
count = mydb.mycollection.count_documents({"firstname": "Marie"})

print(count)
# %% [markdown]

# #### greaterthan($gt),notequals($ne)
# %%

# greaterthan ($gt)
criteria = {"died": {"$gt": "1990"}}
count = mydb.mycollection.count_documents(criteria)
print("died after 1990:  ", count)
# notequals ($ne)
criteria_ne = {"born": {"$ne": "USA"}}
count = mydb.mycollection.count_documents(criteria_ne)
print("not born in USA: ", count)
# %%[markdown]

# ## multiple filters on collection

# %%

# Create a filter for Germany-born laureates who died in the USA and with the first name "Albert"
criteria = {"diedCountry": "USA",
            "bornCountry": "Germany", "firstname": "Albert"}

# Save the count
count = mydb.mycollection.count_documents(criteria)
print(count)

# Save a filter for laureates who died in the USA and were not born there
criteria = {'bornCountry': {"$ne": 'USA'}, 'diedCountry': 'USA'}

# Count them
count = mydb.mycollection.count_documents(criteria)
print(count)

# %% [markdown]

# ## Acessing the substructure of the json for a column in a collection

# %%

# Filter for laureates born in Austria with non-Austria prize affiliation using (.)
criteria = {"bornCountry": "Austria",
            "prizes.affiliations.country": {"$ne": "Austria"}}

# Count the number of such laureates
count = mydb.mycollection.count_documents(criteria)
print(count)

# %%
