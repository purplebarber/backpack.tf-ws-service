from bptf_websocket import BptfWebSocket
from json import load
from asyncio import run


async def main():
    with open('config.json', 'r') as f:
        config = load(f)

    WEBSOCKET_URL = config['websocketURL']
    CLIENT_ID = config['clientId']
    CLIENT_SECRET = config['secret']
    SECURE_CONNECT_BUNDLE = config['secureConnectBundlePath']
    KEYSPACE = config['keyspace']
    TABLE_NAME = config['tableName']

    bptf = BptfWebSocket(CLIENT_ID, CLIENT_SECRET, SECURE_CONNECT_BUNDLE, KEYSPACE, TABLE_NAME, WEBSOCKET_URL)

    try:
        await bptf.parse_websocket_events()
    finally:
        await bptf.close_connection()


if __name__ == '__main__':
    run(main())