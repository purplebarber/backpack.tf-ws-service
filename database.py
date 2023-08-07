from pymongo import MongoClient


class MongoDBManager:
    def __init__(self, connection_string, database_name, collection_name):
        self.client = MongoClient(connection_string)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
        self.create_index()

    def create_index(self) -> None:
        # Creating an index on the 'sku' field for faster queries
        self.collection.create_index([('sku', 1)], unique=True)

    def insert_listing(self, sku, listing_id, listing_data) -> None:
        listing = {
            'sku': sku,
            'listings': {
                str(listing_id): listing_data
            }
        }
        self.collection.update_one({'sku': sku}, {'$set': listing}, upsert=True)

    def get_listing(self, sku, listing_id) -> dict or None:
        result = self.collection.find_one({'sku': sku}, {'listings': {str(listing_id): 1}})
        if result and 'listings' in result:
            return result['listings'].get(str(listing_id))
        return None

    def delete_listing(self, sku, listing_id) -> None:
        self.collection.update_one({'sku': sku}, {'$unset': {f'listings.{str(listing_id)}': 1}})

    def delete_old_listings(self, time) -> None:
        query = {
            'listings.time': {'$lt': time}
        }
        update = {
            '$unset': {'listings.$[element]': 1},
            '$pull': {'listings': None}
        }
        array_filters = [{'element.time': {'$lt': time}}]
        self.collection.update_many(query, update, array_filters=array_filters)

    def close_connection(self) -> None:
        self.client.close()
