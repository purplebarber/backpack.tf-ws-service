import websockets
from json import loads
from asyncio import Future, create_task, gather
from src.database import AsyncMongoDBManager


class BptfWebSocket:
    def __init__(self, connection_string, database_name, collection_name, ws_uri, print_events=False):
        self.mongodb = AsyncMongoDBManager(connection_string, database_name, collection_name)
        self.ws_url = ws_uri
        self.name_dict = dict()
        self.print_events = print_events
        self.do_we_delete_old_listings = True

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
        trade_offers_preferred = payload.get("tradeOffersPreferred")
        buy_out_only = payload.get("buyoutOnly")
        listed_at = payload.get("listedAt")
        bumped_at = payload.get("bumpedAt")
        intent = payload.get("intent")
        user_agent = payload.get("userAgent")

        return {
            "steam_id": steam_id,
            "currencies": currencies,
            "trade_offers_preferred": trade_offers_preferred,
            "buy_out_only": buy_out_only,
            "listed_at": listed_at,
            "bumped_at": bumped_at,
            "intent": intent,
            "user_agent": user_agent
        }

    async def start_websocket(self, websocket_url: str) -> None:
        # Call delete_old_listings when start_websocket starts
        create_task(self.mongodb.delete_old_listings(86_400))

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
        async for message in websocket:
            json_data = loads(message)

            if isinstance(json_data, list):
                tasks = [self.handle_event(thingy.get("payload"), thingy.get("event")) for thingy in json_data]
                await gather(*tasks)
            else:
                await self.handle_event(json_data.get("payload"), json_data.get("event"))

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
