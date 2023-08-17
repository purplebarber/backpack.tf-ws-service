import motor.motor_asyncio
from pymongo import UpdateOne
from time import time


class AsyncMongoDBManager:
    def __init__(self, connection_string, database_name, collection_name):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(connection_string)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]

    async def create_index(self) -> None:
        # Creating an index on the 'sku' field for faster queries
        await self.collection.create_index([('sku', 1)], unique=True)

    async def insert_listing(self, sku, listing_id, listing_data) -> None:
        update_query = {
            '$set': {f'listings.{str(listing_id)}': listing_data},
            '$setOnInsert': {'sku': sku}
        }
        await self.collection.update_one({'sku': sku}, update_query, upsert=True)

    async def get_listing(self, sku, listing_id) -> dict or None:
        result = await self.collection.find_one({'sku': sku}, {'listings': {str(listing_id): 1}})
        if result and 'listings' in result:
            return result['listings'].get(str(listing_id))
        return None

    async def delete_listing(self, sku, listing_id) -> None:
        await self.collection.update_one({'sku': sku}, {'$unset': {f'listings.{str(listing_id)}': 1}})

    async def delete_old_listings(self, time_to_delete: int) -> None:
        bulk_operations = list()
        async for document in self.collection.find():
            listings = document.get('listings', dict())
            updated_listings = dict()
            for listing in listings:
                listing_key = str(listing)
                listing_data = listings[listing_key]
                if type(listing_data) == list:
                    listing_data = listing_data[0]
                listed_at = listing_data.get('listed_at', 0)
                if time() - int(listed_at) < time_to_delete:
                    updated_listings[listing_key] = listing_data

            bulk_operations.append(
                UpdateOne(
                    {'_id': document['_id']},
                    {'$set': {'listings': updated_listings}}
                )
            )

        if bulk_operations:
            await self.collection.bulk_write(bulk_operations)

    async def close_connection(self) -> None:
        self.client.close()
