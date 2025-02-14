from typing import List

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
        cls.data_summary_table_name = cls.config["data_summary_table_name"]
        cls.indexes_table_name = cls.config["indexes_table_name"]
        cls.dimensions = cls.config["dimensions"]
        cls.default_ef_search = cls.config["default_ef_search"]

        cls.param_m = upload_params["hnsw_config"]["m"]
        cls.param_ef_construct = upload_params["hnsw_config"]["ef_construct"]

        cls.cluster = Cluster([cls.config["host"]])
        cls.conn = cls.cluster.connect()

        cls.conn.set_keyspace(cls.keyspace_name)

        cls.insert_query = cls.conn.prepare(f"""
            INSERT INTO {cls.data_table_name} (id, description, embedding, processed) VALUES (?, '', ?, FALSE)
        """)
        cls.update_requested_count_query = cls.conn.prepare(f"""
            UPDATE {cls.data_summary_table_name}
                SET requested_elements_count = requested_elements_count + ?
                WHERE id = 1
        """)
        cls.get_requested_count_query = cls.conn.prepare(f"""
            SELECT requested_elements_count FROM {cls.data_summary_table_name} WHERE id = 1
        """)
        cls.get_processed_count_query = cls.conn.prepare(f"""
            SELECT indexed_elements_count FROM {cls.indexes_table_name} WHERE id = 1
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

            # not_processed_data = []
            # for record in batch:
            #     not_processed_data.append((record.id, "", record.vector))

            # while len(not_processed_data) > 0:
            #     data = list(not_processed_data)
            #     results = execute_concurrent_with_args(cls.conn, cls.insert_query, data, concurrency=50, raise_on_first_error=False)

            #     not_processed_data = []
            #     for i in range(len(results)):
            #         if not results[i][0]:
            #             not_processed_data.append(data[i])

            #     if len(not_processed_data) > 0:
            #         print(f"Retrying {len(not_processed_data)} records")

            # cls.conn.execute(cls.update_requested_count_query, [len(data)])

        except Exception as e:
            print(e)


    @classmethod
    def post_upload(cls, distance):
        try:
            hnsw_distance_type = cls.DISTANCE_MAPPING[distance]
        except KeyError:
            raise IncompatibilityError(f"Unsupported distance metric: {distance}")

        try:
            cls.conn.execute(f"""
                INSERT INTO {cls.indexes_table_name} 
                    (id, indexed_elements_count, param_m, param_ef_construct, param_ef_search, dimension, canceled)
                VALUES (1, 0, {cls.param_m}, {cls.param_ef_construct}, {cls.default_ef_search}, {cls.dimensions}, false);
            """)
            requested = cls.conn.execute(cls.get_requested_count_query).one().requested_elements_count
            processed = cls.conn.execute(cls.get_processed_count_query).one().indexed_elements_count
            while requested != processed:
                sleep(1)
                requested = cls.conn.execute(cls.get_requested_count_query).one().requested_elements_count
                processed = cls.conn.execute(cls.get_processed_count_query).one().indexed_elements_count
                print(f"\rdbg: requested {requested}, processed {processed}", end="")
            print(f"\rdbg: requested {requested}, processed {processed}")
        except Exception as e:
            print(e)
        # TODO: Schedule creating the index
        # cls.conn.execute(
        #     f"CREATE INDEX ON items USING hnsw (embedding {hnsw_distance_type}) WITH (m = {cls.upload_params['hnsw_config']['m']}, ef_construction = {cls.upload_params['hnsw_config']['ef_construct']})"
        # )

        return {}


    @classmethod
    def delete_client(cls):
        cls.cluster.shutdown()
