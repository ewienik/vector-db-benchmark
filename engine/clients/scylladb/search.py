import itertools
import threading
import time
import itertools
import json
from typing import List, Tuple
from multiprocessing import Queue
from http.client import HTTPConnection

import numpy as np
from cassandra.cluster import Cluster

from dataset_reader.base_reader import Query
from engine.base_client.distances import Distance
from engine.base_client.search import BaseSearcher
from engine.clients.scylladb.config import get_db_config
from engine.clients.scylladb.parser import ScyllaDbConditionParser

MAX_PROCESSES = 1000

class ScyllaDbSearcher(BaseSearcher):
    conn = None
    distance = None
    search_params = {}
    parser = ScyllaDbConditionParser()
    scylladb_id = 0
    counter = itertools.count()


    @classmethod
    def next(cls):
        return next(cls.counter) * MAX_PROCESSES + cls.scylladb_id

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        if "scylladb_ids" not in search_params:
            queue = Queue()
            for i in range(MAX_PROCESSES):
                queue.put(i)
            search_params["scylladb_ids"] = queue
        cls.scylladb_id = search_params["scylladb_ids"].get()

        cls.config = get_db_config(host, connection_params)
        cls.keyspace_name = cls.config["keyspace_name"]
        cls.queries_table_name = cls.config["queries_table_name"]
        cls.data_table_name = cls.config["data_table_name"]
        cls.index_name = cls.config["index_name"]
        cls.usearch_host = cls.config["usearch_host"]

        cls.cluster = Cluster([cls.config["host"]])



    @classmethod
    def search_one(cls, query: Query, top) -> List[Tuple[int, float]]:
        request = json.dumps({'embedding': query.vector, 'limit': top})
        headers = {"Content-type": "application/json", "Accept": "application/json"}
        conn = HTTPConnection(f'{cls.usearch_host}:6080')
        conn.request("POST", f'/api/v1/indexes/{cls.keyspace_name}/{cls.index_name}/ann', request, headers)
        response = conn.getresponse().read()
        conn.close()

        try:
            response = json.loads(response)
        except json.JSONDecodeError:
            return []

        if not any(response):
            return []
        return zip(response['primary_keys']['id'], response['distances'])

    @classmethod
    def delete_client(cls):
        cls.cluster.shutdown()
