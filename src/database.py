import motor.motor_asyncio
from pymongo import UpdateOne
from time import time
from pymongo.server_api import ServerApi


class AsyncMongoDBManager:
    def __init__(self, connection_string, database_name, collection_name):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(connection_string, server_api=ServerApi('1'))
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]

    async def create_index(self) -> None:
        # Creating an index on the 'sku' field for faster queries
        await self.collection.create_index([('sku', 1)], unique=True)

    async def insert_listing(self, sku, intent, steamid, listing_data) -> None:
        # First, delete any existing listing for the given steamid and intent
        await self.delete_listing(sku, intent, steamid)
        # Then, insert the new listing
        update_query = {
            '$set': {f'listings.{str(intent)}.{str(steamid)}': listing_data},
            '$setOnInsert': {'sku': sku}
        }
        await self.collection.update_one({'sku': sku}, update_query, upsert=True)

    async def get_listing(self, sku, intent, steamid) -> dict or None:
        result = await self.collection.find_one({'sku': sku}, {'listings': {str(intent): {str(steamid): 1}}})
        if result and 'listings' in result:
            return result['listings'].get(str(intent), {}).get(str(steamid))
        return None

    async def delete_listing(self, sku, intent, steamid) -> None:
        await self.collection.update_one({'sku': sku}, {'$unset': {f'listings.{str(intent)}.{str(steamid)}': 1}})

    async def delete_old_listings(self, time_to_delete: int) -> None:
        current_time = time()
        bulk_operations = list()
        async for document in self.collection.find({'listings.listed_at': {'$lt': current_time - time_to_delete}}):
            listings = document.get('listings', dict())
            updated_listings = dict()
            for intent in listings:
                for steamid in listings[intent]:
                    listing_data = listings[intent][steamid]
                    listed_at = listing_data.get('listed_at', 0)
                    if current_time - int(listed_at) < time_to_delete:
                        if intent not in updated_listings:
                            updated_listings[intent] = {}
                        updated_listings[intent][steamid] = listing_data

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
