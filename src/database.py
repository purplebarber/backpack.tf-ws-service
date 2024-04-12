import motor.motor_asyncio
from pymongo import UpdateOne
from pymongo.server_api import ServerApi


class ListingDBManager:
    def __init__(self, mongo_uri: str, database_name: str, collection_name: str):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri, server_api=ServerApi('1'))
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]

    async def create_index(self) -> None:
        await self.collection.create_index([('name', 1)], unique=True)

    async def insert_listing(self, name: str, intent: str, steamid: str, listing_data: dict) -> None:
        await self.delete_listing(name, intent, steamid)
        update_query = {
            '$push': {"listings": listing_data},
            '$setOnInsert': {'name': name}
        }
        await self.collection.update_one({'name': name}, update_query, upsert=True)

    async def delete_listing(self, name: str, intent: str, steamid: str) -> None:
        await self.collection.update_one(
            {"name": name},
            {"$pull": {"listings": {"steamid": steamid, "intent": intent}}}
        )  # Remove the listing from the document, if it exists

    async def update_many(self, listings_to_update: dict) -> None:
        insert_listings = listings_to_update.get("insert", [])
        delete_listings = listings_to_update.get("delete", [])

        delete_bulk = list()
        insert_bulk = list()

        for operation in insert_listings:
            name = operation["name"]
            intent = operation["intent"]
            steamid = operation["steamid"]
            listing_data = operation["listing_data"]

            delete_bulk.append(
                UpdateOne(
                    {"name": name},
                    {"$pull": {"listings": {"steamid": steamid, "intent": intent}}}
                )
            )

            insert_bulk.append(
                UpdateOne(
                    {'name': name},
                    {
                        '$push': {"listings": listing_data},
                        '$setOnInsert': {'name': name}
                    },
                    upsert=True
                )
            )

        for operation in delete_listings:
            name = operation["name"]
            intent = operation["intent"]
            steamid = operation["steamid"]

            delete_bulk.append(
                UpdateOne(
                    {"name": name},
                    {"$pull": {"listings": {"steamid": steamid, "intent": intent}}}
                )
            )

        await self.collection.bulk_write(delete_bulk) if delete_bulk else None
        await self.collection.bulk_write(insert_bulk) if insert_bulk else None

    async def update_snapshot_time(self, name: str, snapshot_time: float) -> None:
        await self.collection.update_one(
            {"name": name},
            {"$set": {"snapshot_time": snapshot_time}}
        )

    async def get_snapshot_time(self, name: str) -> float:
        snapshot_time = await self.collection.find_one(
            {"name": name},
            {"snapshot_time": 1}
        )
        return snapshot_time.get("snapshot_time") if snapshot_time else 0

    async def get_all_snapshot_times(self) -> dict:
        cursor = self.collection.find({}, {"_id": 0, "name": 1, "snapshot_time": 1})
        snapshot_times = {}
        async for document in cursor:
            snapshot_times[document["name"]] = document.get("snapshot_time", 0)
        return snapshot_times

    async def delete_old_listings(self, max_time: float) -> None:
        await self.collection.update_many(
            {},
            [
                {"$set": {
                    "listings": {
                        "$filter": {
                            "input": "$listings",
                            "cond": {"$gte": ["$$this.updated", max_time]}
                        }
                    }
                }}
            ]
        )

    async def delete_item(self, name: str) -> None:
        await self.collection.delete_one({"name": name})

    async def close_connection(self) -> None:
        self.client.close()
