import configparser
import logging
import psycopg2
from .goodreads_staging_queries import create_staging_schema, drop_staging_tables, create_staging_tables, copy_staging_tables
from .goodreads_warehouse_queries import create_warehouse_schema, drop_warehouse_tables, create_warehouse_tables
from .goodreads_upsert import upsert_queries
from pathlib import Path

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))

class GoodReadsWarehouseDriver:

    def __init__(self):
        self._conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        self._cur = self._conn.cursor()


    def setup_staging_tables(self):
        logging.debug("Creating schema for staging.")
        self.execute_query([create_staging_schema])

        logging.debug("Dropping Staging tables.")
        self.execute_query(drop_staging_tables)

        logging.debug("Creating Staging tables.")
        self.execute_query(create_staging_tables)


    def load_staging_tables(self):
        logging.debug("Populating staging tables")
        self.execute_query(copy_staging_tables)

    def setup_warehouse_tables(self):
        logging.debug("Creating scheam for warehouse.")
        self.execute_query([create_warehouse_schema])

        logging.debug("Creating Warehouse tables.")
        self.execute_query(create_warehouse_tables)


    def perform_upsert(self):
        logging.debug("Performing Upsert.")
        self.execute_query(upsert_queries)


    def execute_query(self, query_list):
        for query in query_list:
            print(query)
            logging.debug(f"Executing Query : {query}")
            self._cur.execute(query)
            self._conn.commit()