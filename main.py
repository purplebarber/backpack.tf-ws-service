from json import load
from database import CassandraManager


def main():
    # Load config
    with open('config.json', 'r') as f:
        config = load(f)

    CLIENT_ID = config['clientId']
    CLIENT_SECRET = config['secret']
    SECURE_CONNECT_BUNDLE = config['secureConnectBundlePath']
    KEYSPACE = config['keyspace']
    TABLE_NAME = config['tableName']

    # Create a new instance of the CassandraManager class
    cassandra_manager = CassandraManager(CLIENT_ID, CLIENT_SECRET, SECURE_CONNECT_BUNDLE, KEYSPACE, TABLE_NAME)
    cassandra_manager.delete_listing("Tough Stuff Muffs", 1)

if __name__ == '__main__':
    main()
