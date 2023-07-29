import websockets
from json import loads
from database import CassandraManager
from six.moves import queue
import asyncio


class BptfWebSocket:
    def __init__(self, client_id, client_secret, secure_connect_bundle_path, keyspace, table_name, ws_uri):
        self.cassandra = CassandraManager(client_id, client_secret, secure_connect_bundle_path, keyspace, table_name)
        self.ws_url = ws_uri

    async def parse_websocket_events(self) -> None:
        async def clear_queue():
            while True:
                try:
                    futures.get_nowait().result()
                except queue.Empty:
                    break

        futures = queue.Queue(maxsize=50)
        async with websockets.connect(self.ws_url, ping_interval=None) as websocket:
            try:
                async for message in websocket:
                    json_data = loads(message)
                    try:
                        if isinstance(json_data, list):
                            for event_data in json_data:
                                future = await self.handle_event(event_data)  # handles the new event format
                        else:
                            future = await self.handle_event(json_data)  # old event-per-frame message format

                    except queue.Full:
                        asyncio.create_task(clear_queue())  # Run clear_queue in a separate asyncio task
                        futures.put(future)

            except websockets.ConnectionClosedError:
                print("Connection closed, reconnecting...")
                return await self.parse_websocket_events()

            finally:
                pass

    async def handle_event(self, json_data: dict) -> None:
        payload = json_data.get("payload", dict())
        if payload.get("appid", 0) != 440:
            return
        listing_event = json_data.get("event")

        if listing_event != "listing-delete" and listing_event != "listing-update":
            return

        listing_id = payload.get("id")
        item_name = payload.get("item").get("name")

        match listing_event:
            case "listing-delete":
                return await self.delete_listing(item_name, listing_id)
            case "listing-update":
                return await self.update_listing(item_name, listing_id, payload)
            case _:
                return

    async def delete_listing(self, item_name: str, listing_id: int) -> None:
        try:
            print(f"Deleting listing {listing_id} for {item_name}")
            await self.cassandra.delete_listing(item_name, listing_id)
        except Exception as e:
            print(f"Error deleting listing {listing_id} for {item_name}: {e}")

    async def update_listing(self, item_name: str, listing_id: int, listing_data: dict) -> None:
        try:
            print(f"Updating listing {listing_id} for {item_name}")
            await self.cassandra.insert_listing(item_name, listing_id, listing_data)
        except Exception as e:
            print(f"Error inserting listing {listing_id} for {item_name}: {e}")

    async def close_connection(self) -> None:
        self.cassandra.close_connection()
