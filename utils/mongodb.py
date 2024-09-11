from pymongo import MongoClient
import json


def save_to_mongodb(uri, article, collection_name, keyword):
    """Saves a DataFrame to a specified MongoDB collection."""
    # Connect to the MongoDB database using the provided URI
    client = MongoClient(uri)

    # Clean up the keyword to remove spaces and convert them to lowercase
    keyword_clean = keyword.replace(' ', '').lower()

    # Select the database based on the cleaned topic and keyword
    db = client[keyword_clean]

    # Select the collection within the chosen database
    collection = db[collection_name]

    if not isinstance(article, dict):
        # Parse the JSON string into a dictionary
        article = json.loads(article)

    # Insert the record into the specified collection
    collection.insert_one(article)

    # Close the connection to the MongoDB server
    client.close()
