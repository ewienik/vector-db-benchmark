import os

SCYLLADB_PORT = int(os.getenv("SCYLLADB_PORT", 9042))
SCYLLADB_USER = os.getenv("SCYLLADB_USER", "")
SCYLLADB_PASSWORD = os.getenv("SCYLLADB_PASSWORD", "")


def get_db_config(host, connection_params):
    return {
        "host": host or "localhost",
        "port": SCYLLADB_PORT,
        "user": SCYLLADB_USER,
        "password": SCYLLADB_PASSWORD,
        "keyspace_name": "vector_benchmark",
        "data_table_name": "vector_items",
        "data_summary_table_name": "vector_items_summary",
        "data_index_table_name": "vector_items_processed",
        "indexes_table_name": "vector_indexes",
        "queries_table_name": "vector_queries",
        **connection_params,
    }
