import json
from typing import List
from http.client import HTTPConnection

import numpy as np
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import BatchStatement, ConsistencyLevel, BatchType

from dataset_reader.base_reader import Record
from engine.base_client import IncompatibilityError
from engine.base_client.distances import Distance
from engine.base_client.upload import BaseUploader
from engine.clients.scylladb.config import get_db_config

from time import sleep


class ScyllaDbUploader(BaseUploader):
    DISTANCE_MAPPING = {
        # TODO: Add support for this in CQL when adding support for vector type
        Distance.L2: "vector_l2_ops",
        Distance.COSINE: "vector_cosine_ops",
    }
    conn = None
    upload_params = {}


    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params):
        cls.config = get_db_config(host, connection_params)
        cls.keyspace_name = cls.config["keyspace_name"]
        cls.data_table_name = cls.config["data_table_name"]
        cls.index_name = cls.config["index_name"]
        cls.data_summary_table_name = cls.config["data_summary_table_name"]
        cls.indexes_table_name = cls.config["indexes_table_name"]
        cls.dimensions = cls.config["dimensions"]
        cls.default_ef_search = cls.config["default_ef_search"]

        cls.param_m = upload_params["hnsw_config"]["m"]
        cls.param_ef_construct = upload_params["hnsw_config"]["ef_construct"]

        cls.usearch_host = cls.config["usearch_host"]

        cls.cluster = Cluster([cls.config["host"]])
        cls.conn = cls.cluster.connect()

        cls.conn.set_keyspace(cls.keyspace_name)

        cls.insert_query = cls.conn.prepare(f"""
            INSERT INTO {cls.data_table_name} (id, embedding) VALUES (?, ?)
        """)
        cls.update_requested_count_query = cls.conn.prepare(f"""
            UPDATE {cls.data_summary_table_name}
                SET requested_elements_count = requested_elements_count + ?
                WHERE id = '{cls.keyspace_name}.{cls.index_name}'
        """)
        cls.get_requested_count_query = cls.conn.prepare(f"""
            SELECT requested_elements_count FROM {cls.data_summary_table_name}
            WHERE id = '{cls.keyspace_name}.{cls.index_name}'
        """)

        cls.upload_params = upload_params


    @classmethod
    def upload_batch(cls, batch: List[Record]):
        try:
            batch_statement = BatchStatement(consistency_level=ConsistencyLevel.ANY, 
                                             batch_type=BatchType.UNLOGGED)
            for record in batch:
                batch_statement.add(cls.insert_query, (record.id, record.vector))
            cls.conn.execute(batch_statement)
            cls.conn.execute(cls.update_requested_count_query, [len(batch)])

        except Exception as e:
            print(e)


    @classmethod
    def get_index_count(cls):
        while True:
            conn = HTTPConnection(f'{cls.config["usearch_host"]}:6080')
            conn.request("GET", f'/api/v1/indexes/{cls.keyspace_name}/{cls.index_name}/count')
            response = conn.getresponse()
            if response.status == 200:
                break;
            conn.close()
            sleep(1)
        response = response.read()
        conn.close()
        return json.loads(response)

    @classmethod
    def post_upload(cls, distance):
        try:
            hnsw_distance_type = cls.DISTANCE_MAPPING[distance]
        except KeyError:
            raise IncompatibilityError(f"Unsupported distance metric: {distance}")

        try:
            #cls.conn.execute(f"""
            #    CREATE INDEX {cls.index_name} ON {cls.data_table_name}(embedding) USING 'dummy-vector-backend'
            #""")
            requested = cls.conn.execute(cls.get_requested_count_query).one().requested_elements_count
            processed = cls.get_index_count()
            while requested != processed:
                sleep(1)
                requested = cls.conn.execute(cls.get_requested_count_query).one().requested_elements_count
                processed = cls.get_index_count()
                print(f"\rdbg: requested {requested}, processed {processed}", end="")
            print(f"\rdbg: requested {requested}, processed {processed}")
        except Exception as e:
            print(e)

        return {}


    @classmethod
    def delete_client(cls):
        cls.cluster.shutdown()
