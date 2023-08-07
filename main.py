from bptf_websocket import BptfWebSocket
from json import load
from asyncio import run
from sku.parser import Sku


async def main():
    Sku.update_autobot_pricelist()
    with open('config.json', 'r') as f:
        config = load(f)

    WEBSOCKET_URL = config['websocket_url']
    CONNECTION_STRING = config['connection_string']
    DATABASE_NAME = config['database_name']
    COLLECTION_NAME = config['collection_name']
    PRINT_EVENTS = config['print_events']

    bptf = BptfWebSocket(CONNECTION_STRING, DATABASE_NAME, COLLECTION_NAME, WEBSOCKET_URL, PRINT_EVENTS)

    try:
        print("Starting websocket...") if PRINT_EVENTS else None
        await bptf.parse_websocket_events()
    finally:
        print("Closing connection...") if PRINT_EVENTS else None
        await bptf.close_connection()


if __name__ == '__main__':
    run(main())
