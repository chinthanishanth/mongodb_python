

# %% [markdown]

# ## Loading required packages and creating database and collections

# %%

# importing required packages
from bson.regex import Regex
import pymongo
import json

# %%

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

# ## $in and $nin operators

# %%

count = mydb.mycollection.count_documents(
    {"prizes.category": {"$ne": ["physics", "chemistry", "medicine"]}})

print(count)

count = mydb.mycollection.count_documents(
    {"prizes.category": {"$nin": ["physics", "chemistry", "medicine"]}})

print(count)


# %%[markdown]

# ## $elemMatch operator matches documents that contain an array field with at least one element that matches all the specified query criteria.
# ## this operator moster used for arrays in json

# %%

# matching the documents in the prizes which matches the criteria category = physics and share =2
mydb.mycollection.count_documents(
    {"prizes": {"$elemMatch": {"category": "physics", "year": {"$gt": "1947"}}}})

# %%
# Save a filter for mycollection with unshared prizes
unshared = {
    "prizes": {"$elemMatch": {
        "category": {"$nin": ["physics", "chemistry", "medicine"]},
        "share": "1",
        "year": {"$gte": "1945"},
    }}}

# Save a filter for mycollection with shared prizes
shared = {
    "prizes": {"$elemMatch": {
        "category": {"$nin": ["physics", "chemistry", "medicine"]},
        "share": {"$ne": "1"},
        "year": {"$gte": "1945"},
    }}}

ratio = mydb.mycollection.count_documents(
    unshared) / mydb.mycollection.count_documents(shared)
print(ratio)

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

# ## Acessing the substructure of the json for a column in a collection using (.)

# %%

# Filter for laureates born in Austria with non-Austria prize affiliation
criteria = {"bornCountry": "Austria",
            "prizes.affiliations.country": {"$ne": "Austria"}}

# Count the number of such laureates
count = mydb.mycollection.count_documents(criteria)
print(count)

# %%

# Save a filter for organization laureates with prizes won before 1945
before = {
    "gender": "org",
    "prizes.year": {"$lt": "1945"},
}

# Save a filter for organization laureates with prizes won in or after 1945
in_or_after = {
    "gender": "org",
    "prizes.year": {"$gte": "1945"},
}

n_before = mydb.mycollection.count_documents(before)
print(n_before)
n_in_or_after = mydb.mycollection.count_documents(in_or_after)
print(n_in_or_after)

# %%[markdown]

# ## $exists operator to check weather key present in json structure

# %%

# ## checking the count of  born key not present in all documents - returns 0 indicate born key present in all documents
criteria = {"born": {"$exists": False}}

mydb.mycollection.count_documents(criteria)

# %%[markdown]

# ## finding the distinct values for the for keys in document in a collection using distinct() method


# %%
# finding the distinct values for the countrys in the prizes array
mydb.mycollection.distinct("prizes.affiliations.country")


# %%

# finding the countries who died country is not equal to the born country

countries = set(mydb.mycollection.distinct("diedCountry")) - \
    set(mydb.mycollection.distinct("bornCountry"))
print(countries)

# %%

# find the number of distinct vales for affliation for prizes

count = len((mydb.mycollection.distinct("prizes.affiliations.country")))
print(count)

# %%[markdown]

# ## finding the distinct values for  the documents after applying filter
# distinct method takes the optional filter parameter to filter the documents before applying the
# distinct criteria

# %%

# In which countries have USA-born laureates had affiliations for their prizes
mydb.mycollection.distinct("prizes.affiliations.country", {
                           "bornCountry": "USA"})


# %%[markdown]

# ## filtering the documents using regular expressions

# %%

# Filter for laureates with "Germany" in their "bornCountry" value
criteria = {"bornCountry": Regex("Germany")}
print(set(mydb.mycollection.distinct("bornCountry", criteria)))

# %%
