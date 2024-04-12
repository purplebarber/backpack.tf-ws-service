from src.bptf_websocket import BptfWebSocket
from json import load
from asyncio import run


async def main():
    with open('config.json', 'r') as f:
        config = load(f)

    WEBSOCKET_URL = config['websocket_url']
    MONGO_URI = config['mongo_uri']
    DATABASE_NAME = config['database_name']
    COLLECTION_NAME = config['collection_name']
    PRINT_EVENTS = config['print_events']
    BPTF_TOKEN = config['bptf_token']
    PRIO_ITEMS = config['prioritized_items']

    bptf = BptfWebSocket(MONGO_URI,
                         DATABASE_NAME,
                         COLLECTION_NAME,
                         WEBSOCKET_URL,
                         PRINT_EVENTS,
                         BPTF_TOKEN,
                         PRIO_ITEMS)

    try:
        print("Starting websocket...") if PRINT_EVENTS else None
        await bptf.start_websocket(WEBSOCKET_URL)

    finally:
        print("Closing connection...") if PRINT_EVENTS else None
        await bptf.close_connection()


if __name__ == '__main__':
    run(main())
