

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
# #### filtering the documents using regular expressions can be done in two ways<br>
# #### one by using bson.regex package <br>
# #### another by using mongodb $regex package <br>

# %%

# filtering documents using bson.regex and then applying distinct filter
criteria = {"bornCountry": Regex("Germany")}
print(set(mydb.mycollection.distinct("bornCountry", criteria)))

# %%

# filtering the documents using $regex operator
list(mydb.mycollection.find({"prizes.motivation": {"$regex": "radiation"}}))

# %%[markdown]

# ## By default, queries in MongoDB return all fields in matching documents.
# ## To limit the amount of data that MongoDB sends to applications,
# ## you can include a projection document to specify or restrict fields to return

# #### `1` denotes  field should be present while fetching data 
# #### `0` denotes field should not be present while fetching data

# %%

# selecting only firstname and affiliation country from prizes array
docs = list(mydb.mycollection.find({},{"firstname": 1,"prizes.affiliations.country": 1,"_id":0}))
print(docs)

#%%[markdown]

# ## sorting the projected fields using sorted 

# %%

# sorting the output by year in prizes array and born field
docs = list(mydb.mycollection.find(
    {"born": {"$gte": "1900"}, "prizes.year": {"$gte": "1954"}},
    {"born": 1, "prizes.year": 1, "_id": 0}, # projected fields
    sort=[("prizes.year", 1), ("born", -1)])) # sorted by year and born

print(docs)

#%%

# Sort by ascending year
sort_spec = [("year", 1)]

cursor = mydb.mycollection1.find({"category": "physics"}, ["year", "laureates.firstname", "laureates.surname"], sort=sort_spec)
docs = list(cursor)

print(docs)

#%%[markdown]

# ## indexing 

# #### Indexes support the efficient execution of queries in MongoDB. Without indexes, 
# #### MongoDB must perform a collection scan, i.e. scan every document in a collection, 
# #### to select those documents that match the query statement. If an appropriate index exists for a query,
# #### MongoDB can use the index to limit the number of documents it must inspect.
# %%

# creating a index to increase th perfoemance of the querying 

mydb.mycollection.create_index([("prizes.year",1)])

docs = [doc["firstname"] for doc in mydb.mycollection.find({"prizes.year":{"$gt":"1947"}})]

print(docs)

# %%[markdown]

# ## creating indexes and writing the complex queries

#%%

# creating a compund index and projection to print only selected items
compound_index = [("prizes.year",1),("bornCountry",1)]
mydb.mycollection.create_index(compound_index)

# fetching all distinct categories for each country
for country in mydb.mycollection.distinct("bornCountry"):
    for category in mydb.mycollection.distinct("prizes.category",{"bornCountry":country}):
        #output = "country: "+ "{bornCountry} \nprizes: {prizes}".format(**category)
        output = "country: "+country+", category: " +category
        print(output)


#%% 

#  both  country of birth ("bornCountry") and a country of affiliation for one or more of their prizes ("prizes.affiliations.country")
#  are same ,find the top 5 countries having most counts



from collections import Counter
# collecting results in set
docs  = {
 country : mydb.mycollection.count_documents({
        "bornCountry": country,
        "prizes.affiliations.country": country
    }) for country in  mydb.mycollection.distinct("bornCountry")
}

print(docs)

print("\n =================== priting the top 5 counries with most count ===================\n")

# display the top five countries using Counter in collection package 

five_most_common = Counter(docs).most_common(5)
print(five_most_common)

#%%

