from pymongo import MongoClient
client = MongoClient()
db = client.logic
collection = db.Term

if __name__ == "__main__":
	collection.find()