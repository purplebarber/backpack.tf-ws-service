import websockets
from json import loads
from database import MongoDBManager
from sku.parser import Sku


class BptfWebSocket:
    def __init__(self, connection_string, database_name, collection_name, ws_uri, print_events=False):
        self.mongodb = MongoDBManager(connection_string, database_name, collection_name)
        self.ws_url = ws_uri
        self.name_dict = dict()
        self.print_events = print_events

    async def print_event(self, listing_event, payload) -> None:
        if not self.print_events:
            return

        print(f"Event: {listing_event}")
        print(f"Payload: {payload}")

    async def reformat_event(self, payload: dict) -> dict:
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

        if not (buy_out_only or trade_offers_preferred):
            print(f"Invalid listing: {payload}") if self.print_events else None
            return dict()

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

    async def parse_websocket_events(self) -> None:
        async with websockets.connect(self.ws_url, ping_interval=None) as websocket:
            try:
                print("Connected to websocket...") if self.print_events else None
                async for message in websocket:
                    json_data = loads(message)
                    try:
                        if isinstance(json_data, list):
                            for event_data in json_data:
                                await self.handle_event(event_data)  # handles the new event format
                        else:
                            await self.handle_event(json_data)  # old event-per-frame message format

                    except Exception as e:
                        print(e)
                        pass

            except websockets.ConnectionClosedError:
                print("Connection closed, reconnecting...")
                return await self.parse_websocket_events()

            finally:
                pass

    async def handle_event(self, json_data: dict) -> dict:
        payload = json_data.get("payload", dict())

        if payload.get("appid", 0) != 440:
            return dict()

        listing_event = json_data.get("event")

        if listing_event != "listing-delete" and listing_event != "listing-update":
            return dict()

        listing_id = payload.get("id")
        item_name = payload.get("item").get("name")
        if item_name not in self.name_dict:
            sku = Sku.name_to_sku(item_name)
            self.name_dict[item_name] = sku
        else:
            sku = self.name_dict[item_name]

        if listing_event == "listing-update":
            parsed_payload = await self.reformat_event(payload)
            if not parsed_payload:
                return dict()
            await self.print_event(listing_event, parsed_payload)
            self.mongodb.insert_listing(sku, listing_id, parsed_payload)

        elif listing_event == "listing-delete":
            self.mongodb.delete_listing(sku, listing_id)
            await self.print_event(listing_event, payload)

        return dict()

    async def close_connection(self) -> None:
        self.mongodb.close_connection()
