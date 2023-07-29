import websockets
from json import loads, load, dumps
from asyncio import run


with open('config.json', 'r') as f:
    config = load(f)

WEBSOCKET_URL = config['websocketURL']


async def parse_websocket_events() -> None:
    async with websockets.connect(WEBSOCKET_URL) as websocket:
        async for message in websocket:
            json_data = loads(message)
            payload = json_data.get("payload", dict())
            if payload.get("appid", 0) != 440:
                continue
            listing_event = json_data.get("event")

            if listing_event != "listing-delete" and listing_event != "listing-update":
                continue

            listing_id = payload.get("id")
            item_name = payload.get("item").get("name")

            if isinstance(payload, list):
                for event in payload:
                    handle_event(event)
            else:
                handle_event(payload)


def handle_event():
    pass

run(parse_websocket_events())
