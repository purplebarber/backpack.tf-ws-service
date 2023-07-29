from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from json import loads, dumps


class CassandraManager:
    def __init__(self, client_id, client_secret, secure_connect_bundle_path, keyspace, table_name):
        cloud_config = {'secure_connect_bundle': secure_connect_bundle_path}
        auth_provider = PlainTextAuthProvider(client_id, client_secret)
        self.cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = self.cluster.connect(keyspace)  # Connect to the specified keyspace
        self.keyspace = keyspace
        self.table_name = table_name
        self.create_table()

    def create_table(self) -> None:
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.{self.table_name} (
                sku text PRIMARY KEY,
                listings map<text, text>
            )
        """
        self.session.execute(query)

    def insert_listing(self, sku, listing_id, listing_data) -> None:
        query = f"""
            UPDATE {self.keyspace}.{self.table_name}
            SET listings[%s] = %s
            WHERE sku = %s
        """
        # Use SimpleStatement with the map data for proper handling
        statement = SimpleStatement(query)
        self.session.execute(statement, (str(listing_id), dumps(listing_data), str(sku)))

    def get_listing(self, sku, listing_id) -> dict | None:
        query = f"SELECT listings['{str(listing_id)}'] FROM {self.keyspace}.{self.table_name} WHERE sku = '{str(sku)}'"
        result = self.session.execute(query)
        return loads(result.one()[0]) if result else None

    def delete_listing(self, sku, listing_id) -> None:
        query = f"""
            UPDATE {self.keyspace}.{self.table_name}
            SET listings = listings - {{ '{str(listing_id)}' }}
            WHERE sku = '{str(sku)}'
        """
        self.session.execute(query)

    def close_connection(self) -> None:
        self.session.shutdown()
        self.cluster.shutdown()
