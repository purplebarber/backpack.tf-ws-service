import websockets
from json import loads
from asyncio import Future, create_task, sleep
from src.database import ListingDBManager
from httpx import AsyncClient
from time import time


class BptfWebSocket:
    def __init__(self, mongo_uri: str,
                 database_name: str = "bptf",
                 collection_name: str = "listings",
                 ws_uri: str = "wss://ws-backpack.tf/ws",
                 print_events: bool = False,
                 bptf_token: str = None,
                 prioritized_items: list[str] = None):

        if not mongo_uri:
            raise ValueError("Mongo URI is required")

        self.mongodb = ListingDBManager(mongo_uri, database_name, collection_name)
        self.ws_url = ws_uri
        self.name_dict = dict()
        self.print_events = print_events
        self.do_we_delete_old_listings = True
        self.snapshot_times = dict()
        self.bptf_token = bptf_token
        self.prioritized_items = set(prioritized_items) if prioritized_items else set()
        self.http_client = AsyncClient()

    async def print_event(self, thing_to_print) -> None:
        if not self.print_events:
            return

        print(thing_to_print)

    @staticmethod
    async def reformat_event(payload: dict) -> dict:
        if not payload:
            return dict()

        steam_id = payload.get("steamid")
        currencies = payload.get("currencies")
        buy_out_only = payload.get("buyoutOnly")

        if payload.get('bumpedAt'):  # websocket
            listed_at = payload.get("listedAt")
            bumped_at = payload.get("bumpedAt")
            trade_offers_preferred = payload.get("tradeOffersPreferred")
        else:  # snapshots
            listed_at = payload.get("timestamp")
            bumped_at = payload.get("bump")
            trade_offers_preferred = payload.get("offers")

        intent = payload.get("intent")
        user_agent = payload.get("userAgent")
        item = payload.get('item')
        details = payload.get('details')
        only_buyout = payload.get('buyout', True)

        return {
            "steamid": steam_id,
            "currencies": currencies,
            "trade_offers_preferred": trade_offers_preferred,
            "buy_out_only": buy_out_only,
            "listed_at": listed_at,
            "bumped_at": bumped_at,
            "intent": intent,
            "user_agent": user_agent,
            "item": item,
            "details": details,
            "only_buyout": only_buyout
        }

    async def update_snapshot(self, item_name: str) -> None:
        snap_request = await self.http_client.get(
            "https://backpack.tf/api/classifieds/listings/snapshot",
            params={
                "token": self.bptf_token,
                "sku": item_name,
                "appid": "440"
            }
        )

        if snap_request.status_code == 429:
            await sleep(5)
            return

        if snap_request.status_code != 200:
            return

        snapshot = snap_request.json()

        if not snapshot:
            return

        listings = snapshot.get("listings")
        snapshot_time = snapshot.get("createdAt")

        if not listings or not snapshot_time:
            return

        operations = {"insert": list(), "delete": list()}

        for listing in listings:
            listing_data = await self.reformat_event(listing)
            if not listing_data:
                continue

            operations["insert"].append({
                "name": item_name,
                "intent": listing_data.get("intent"),
                "steamid": listing_data.get("steam_id"),
                "listing_data": listing_data
            })

        await self.mongodb.delete_item(item_name)
        await self.mongodb.update_many(operations)
        await self.mongodb.update_snapshot_time(item_name, snapshot_time)
        self.snapshot_times[item_name] = snapshot_time

    async def refresh_snapshots(self) -> None:
        await self.print_event("Refreshing snapshots...")
        self.snapshot_times = await self.mongodb.get_all_snapshot_times()

        while True:
            oldest_items = sorted(self.snapshot_times.items(), key=lambda x: x[1])[:10]
            oldest_items = [item[0] for item in oldest_items]

            oldest_prioritized_items = sorted(
                ((k, v) for k, v in self.snapshot_times.items() if k in self.prioritized_items.copy()),
                key=lambda x: x[1]
            )[:10]
            oldest_prioritized_items = [item[0] for item in oldest_prioritized_items][:10]

            for item in oldest_items:
                await self.update_snapshot(item)
                await self.print_event(f"Refreshed snapshot for {item}")
                await sleep(1)

            for item in oldest_prioritized_items:
                await self.update_snapshot(item)
                await self.print_event(f"Refreshed snapshot for {item}")
                await sleep(1)

            await sleep(1)

    async def start_websocket(self, websocket_url: str) -> None:
        await self.mongodb.delete_old_listings(172800 + time())  # 2 days
        create_task(self.refresh_snapshots())

        # Create index on name
        await self.mongodb.create_index()

        while True:
            try:
                async with websockets.connect(
                        websocket_url,
                        max_size=None,
                        ping_interval=60,
                        ping_timeout=120
                ) as websocket:
                    await self.handle_websocket(websocket)
                    await Future()  # keep the connection open
            except (websockets.ConnectionClosedError, websockets.ConnectionClosedOK, websockets.ConnectionClosed) as e:
                await self.print_event(f"Connection closed: {e}, trying to reconnect")
                continue

            except KeyboardInterrupt:
                break

    async def handle_websocket(self, websocket):
        await self.print_event("Connected to backpack.tf websocket!")
        listing_count = 0

        async for message in websocket:
            await self.print_event(f"Received {listing_count} events")

            json_data = loads(message)

            if isinstance(json_data, list):
                create_task(self.handle_list_events(json_data))
                listing_count += len(json_data)
            else:
                create_task(self.handle_event(json_data, json_data.get("event")))
                listing_count += 1

    async def handle_event(self, data: dict, event: str) -> None:
        # If no data is provided, exit the function
        if not data:
            return

        item_name = data.get("item", dict()).get("name")

        # Depending on the event type, perform different actions
        match event:
            # If the event is a listing update
            case "listing-update":
                # Process the listing
                await self.process_listing(data, item_name)

            # If the event is a listing deletion
            case "listing-delete":
                # Process the deletion
                await self.process_deletion(item_name, data.get("intent"), data.get("steamid"))

            # If the event is neither a listing update nor a deletion, exit the function
            case _:
                return

    async def handle_list_events(self, events: list) -> None:
        listings_to_update = {"insert": list(), "delete": list()}

        for event in events:
            data = event.get("payload", dict())
            item_name = data.get("item", dict()).get("name")

            if not item_name:
                continue

            if not data:
                continue

            match event.get("event"):
                case "listing-update":
                    listing_data = await self.reformat_event(data)
                    if not listing_data:
                        continue

                    listings_to_update["insert"].append({
                        "name": item_name,
                        "intent": listing_data.get("intent"),
                        "steamid": listing_data.get("steam_id"),
                        "listing_data": listing_data
                    })

                case "listing-delete":
                    listings_to_update["delete"].append({
                        "name": item_name,
                        "intent": data.get("intent"),
                        "steamid": data.get("steamid")
                    })

                case _:
                    continue

        await self.mongodb.update_many(listings_to_update)

    async def process_listing(self, data: dict, item_name: str) -> None:
        # Reformat the data
        listing_data = await self.reformat_event(data)
        # If the data is empty, exit the function
        if not listing_data:
            return

        # Insert the listing into the database
        await self.mongodb.insert_listing(item_name, listing_data.get("intent"),
                                          listing_data.get("steam_id"), listing_data)
        await self.print_event(f"listing-update for {item_name} with intent {listing_data.get('intent')}"
                               f" and steamid {listing_data.get('steam_id')}")

    async def process_deletion(self, item_name: str, intent: str, steamid: str) -> None:
        # Delete the listing from the database
        await self.mongodb.delete_listing(item_name, intent, steamid)
        await self.print_event(f"listing-delete for {item_name} with intent {intent} and steamid {steamid}")

    async def close_connection(self) -> None:
        await self.mongodb.close_connection()
