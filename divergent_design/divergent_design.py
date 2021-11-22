import numpy as np
import pickle

from dbms.cost_evaluation import CostEvaluation
from dbms.index_selection import IndexSelection
from dbms.postgres_connector import PostgresDatabaseConnector
from dbms.query_generator import QueryGenerator
from dbms.table_generator import TableGenerator
from dbms.workload import *


class DivergentDesign:
    def __init__(self, db_name, num_replicas, config):
        self.db_name = db_name
        self.config = config
        self.benchmark_name = config['benchmark']
        self.scale_factor = config['scale_factor']
        self.num_replicas = num_replicas
        generating_connector = PostgresDatabaseConnector(None, autocommit=True)
        self.table_generator = TableGenerator(
            self.benchmark_name, 0.001, generating_connector,
        )

        self.db_connector = PostgresDatabaseConnector(self.db_name, autocommit=True)
        # self.db_connector.enable_simulation()
        self.db_connector.drop_indexes()

        # self.cost_evaluation = CostEvaluation(self.db_connector, cost_estimation="actual_runtimes")
        self.cost_evaluation = CostEvaluation(self.db_connector)
        self.index_selection = IndexSelection(self.db_name, config=config['index_selection'])

        self.queries = []
        self.workload = {}
        self.workload_partition = [{} for i in range(self.num_replicas)]
        self.configurations = []
        self.workload_size = config['divergent_design']['workload_size']
        self.factor_n = config['divergent_design']['factor']
        assert self.factor_n <= self.num_replicas

    def no_index(self):
        self._generator()

        for q in self.queries:
            cost = self.cost_evaluation.calculate_cost(Workload([q]), [])
            indexes, what_if, _, _ = self.index_selection.run(Workload([q]))
            index_cost = self.cost_evaluation.calculate_cost(Workload([q]), indexes)
            print(q, cost)
            print(indexes, index_cost)

    def uniform_design(self):
        self._generator()
        for i in range(self.num_replicas):
            for j in range(len(self.queries)):
                self.workload_partition[i][self.queries[j].nr] = self.workload_size

        indexes = self._index(self.workload_partition[0])

        replicas_cost = np.zeros(self.num_replicas, dtype=np.int64)
        for q in self.queries:
            cost = self.cost_evaluation.calculate_cost(Workload([q]), indexes)
            for i in range(self.num_replicas):
                replicas_cost[i] += cost * (self.workload_size / self.num_replicas)

        print("uniform design", replicas_cost)

        self.workload_partition = [{} for i in range(self.num_replicas)]
        self.configurations = []
        return indexes

    def run(self):
        raise NotImplementedError

    def _index(self, workload):
        queries = []
        for q in self.queries:
            if q.nr in workload:
                queries.extend([q] * workload[q.nr])
        indexes, what_if, _, _ = self.index_selection.run(Workload(queries))
        return indexes

    def _configurations(self, workload_partition):
        self.index_selection = IndexSelection(self.db_name, config=self.config['index_selection'])
        configurations = []
        for i in range(len(workload_partition)):
            configurations.append(self._index(workload_partition[i]))

        # for cfg in configurations:
        #     print(len(cfg), cfg)

        return configurations

    def _evaluation(self, configurations):
        self.cost_evaluation = CostEvaluation(self.db_connector)
        self.db_connector.drop_indexes()
        queries_replicas_cost = []
        queries_best_n = []
        replicas_cost = np.zeros(self.num_replicas, dtype=np.int64)
        for q in self.queries:
            best_n_replicas, query_replicas_cost = self._best_n(q, configurations)
            queries_replicas_cost.append(query_replicas_cost)
            queries_best_n.append(best_n_replicas)
            n = len(best_n_replicas)
            for replica_id in best_n_replicas:
                replicas_cost[replica_id] += query_replicas_cost[replica_id] * (self.workload_size / n)
        return queries_replicas_cost, queries_best_n, replicas_cost

    def _best_n(self, query, configurations):
        best_n_replicas = []
        query_replicas_cost = np.zeros(self.num_replicas)
        for replica_id in range(self.num_replicas):
            query_replicas_cost[replica_id] = self.cost_evaluation.calculate_cost(Workload([query]),
                                                                                  configurations[replica_id])
        sort_replicas_id = np.argsort(query_replicas_cost)
        sort_replicas_cost = query_replicas_cost.take(sort_replicas_id)

        for i in range(self.factor_n):
            best_n_replicas.append(sort_replicas_id[i])

        for i in range(self.factor_n, self.num_replicas):
            if sort_replicas_cost[i] <= sort_replicas_cost[i - 1]:
                best_n_replicas.append(sort_replicas_id[i])
            else:
                break

        return best_n_replicas, query_replicas_cost

    def _generator(self):
        if self.benchmark_name == 'tpcds':
            with open('queries_tpcds', 'rb') as file:
                queries = pickle.load(file)
                self.queries = queries
        else:
            with open('queries', 'rb') as file:
                queries = pickle.load(file)
                self.queries = queries
