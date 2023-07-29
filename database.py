from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from json import loads, dumps


class CassandraManager:
    def __init__(self, client_id, client_secret, secure_connect_bundle_path, keyspace, table_name):
        cloud_config = {
            'secure_connect_bundle': secure_connect_bundle_path
        }
        auth_provider = PlainTextAuthProvider(client_id, client_secret)
        self.cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, control_connection_timeout=30,
                               connect_timeout=30)
        self.session = self.cluster.connect(keyspace)
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

    async def insert_listing(self, sku, listing_id, listing_data) -> None:
        query = f"""
            UPDATE {self.keyspace}.{self.table_name}
            SET listings[%s] = %s
            WHERE sku = %s
        """

        statement = SimpleStatement(query)
        return self.session.execute_async(statement, (str(listing_id), dumps(listing_data), str(sku)))

    async def get_listing(self, sku, listing_id) -> dict | None:
        query = f"SELECT listings['{str(listing_id)}'] FROM {self.keyspace}.{self.table_name} WHERE sku = '{str(sku)}'"
        result = self.session.execute(query)
        try:
            return loads(result.one()[0]) if result else None
        except TypeError:
            return None

    async def delete_listing(self, sku, listing_id) -> None:
        listing_id_set = {str(listing_id)}

        query = f"""
            UPDATE {self.keyspace}.{self.table_name}
            SET listings = listings - %s
            WHERE sku = %s
        """
        statement = SimpleStatement(query)
        return self.session.execute_async(statement, (listing_id_set, str(sku)))

    def close_connection(self) -> None:
        self.session.shutdown()
        self.cluster.shutdown()
