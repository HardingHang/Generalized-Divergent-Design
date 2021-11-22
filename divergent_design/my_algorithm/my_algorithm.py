import copy

import numpy as np
from sklearn import cluster

from divergent_design.divergent_design import DivergentDesign
from divergent_design.my_algorithm.query_encoder import QueryEncoder


class MyAlgorithm(DivergentDesign):
    def __init__(self, db_name, num_replicas, config):
        DivergentDesign.__init__(self, db_name, num_replicas, config=config)
        self.encoder = QueryEncoder(self.table_generator.tables, self.table_generator.columns)
        self.encoded_queries = None
        self.cfg = config

    def run(self):
        print("run")
        self._initialize()

        c = 1
        while c:
            c = self._update()

        _, _, replicas_cost = self._evaluation(self.configurations)
        print("my algorithm", replicas_cost)

    def _initialize(self):
        self._generator()
        encoded_queries = []
        for q in self.queries:
            encoded_queries.append(self.encoder.encode(q.text))

        self.encoded_queries = np.array(encoded_queries)

        # clustering = cluster.AgglomerativeClustering(n_clusters=self.num_replicas).fit(self.encoded_queries)
        # clustering = cluster.SpectralClustering(n_clusters=self.num_replicas).fit(self.encoded_queries)
        clustering = cluster.SpectralClustering(n_clusters=self.num_replicas).fit(self.encoded_queries)
        # clustering = cluster.KMeans(n_clusters=self.num_replicas).fit(self.encoded_queries)
        query_labels = clustering.labels_

        workload_cluster = [[] for i in range(self.num_replicas)]
        for i in range(len(self.queries)):
            self.workload_partition[query_labels[i]][self.queries[i].nr] = self.workload_size
        # self.workload_partition = [
        #     {1: 1, 2: 1, 3: 1, 4: 1, 6: 1, 7: 1, 8: 1, 10: 1, 11: 1, 12: 1, 13: 1, 14: 1, 16: 1, 17: 1, 18: 1, 19: 1,
        #      20: 1, 21: 1, 22: 1, 24: 1, 25: 1, 28: 1, 29: 1, 30: 1, 31: 1, 32: 1, 34: 1, 35: 1, 36: 1, 37: 1, 38: 1,
        #      39: 1, 40: 1, 41: 1, 42: 1, 43: 1, 44: 1, 45: 1, 46: 1, 47: 1, 48: 1, 49: 1, 50: 1, 51: 1, 52: 1, 53: 1,
        #      54: 1, 55: 1, 56: 1, 57: 1, 58: 1, 59: 1, 60: 1, 61: 1, 62: 1, 63: 1, 64: 1, 65: 1, 66: 1, 68: 1, 69: 1,
        #      70: 1, 71: 1, 72: 1, 73: 1, 74: 1, 75: 1, 76: 1, 77: 1, 78: 1, 79: 1, 80: 1, 81: 1, 82: 1, 83: 1, 84: 1,
        #      85: 1, 86: 1, 87: 1, 88: 1, 89: 1, 90: 1, 91: 1, 92: 1, 93: 1, 94: 1, 95: 1, 96: 1, 97: 1},
        #     {9: 1, 33: 1, 67: 1, 63: 1}, {15: 1, 23: 1, 27: 1, 5: 1, 26: 1}]

        if self.cfg["index_selection"]["parameters"]["budget_MB"] == 300:
            print("300")
            self.workload_partition = [
                {5: 1, 6: 1, 9: 1, 10: 1, 14: 1, 15: 1, 22: 1, 23: 1, 30: 1, 36: 1, 41: 1, 43: 1, 46: 1, 48: 1, 54: 1,
                 56: 1, 58: 1, 60: 1, 63: 1, 82: 1, 73: 1},
                {1: 1, 2: 1, 3: 1, 4: 1, 7: 1, 11: 1, 12: 1, 19: 1, 20: 1, 24: 1, 26: 1, 27: 1, 28: 1, 29: 1, 32: 1, 33: 1,
                 35: 1, 37: 1, 38: 1, 39: 1, 40: 1, 42: 1, 44: 1, 45: 1, 47: 1, 49: 1, 50: 1, 51: 1, 52: 1, 53: 1, 55: 1,
                 57: 1, 59: 1, 61: 1, 62: 1, 64: 1, 65: 1, 66: 1, 67: 1, 68: 1, 69: 1, 70: 1, 71: 1, 72: 1, 73: 1, 74: 1,
                 76: 1, 77: 1, 78: 1, 79: 1, 80: 1, 81: 1, 83: 1, 84: 1, 85: 1, 86: 1, 87: 1, 88: 1, 89: 1, 91: 1},
                {8: 1, 13: 1, 16: 1, 17: 1, 18: 1, 21: 1, 25: 1, 31: 1, 34: 1, 75: 1, 90: 1, 29: 1}]
        print(self.workload_partition)

        self.configurations = self._configurations(self.workload_partition)

    def _update(self):
        self.db_connector.exec_fetch("SELECT * FROM hypopg_reset();")

        queries_replicas_cost, queries_best_n, replicas_cost = self._evaluation(self.configurations)
        sorted_replica_id_by_cost = np.argsort(-replicas_cost)
        replica_id_with_max_cost = sorted_replica_id_by_cost[0]
        replica_id_with_min_cost = sorted_replica_id_by_cost[-1]

        new_workload_partition = None
        new_configurations = None
        query_id = None
        current_max_replica_cost = np.max(replicas_cost)

        for q_id in self.workload_partition[replica_id_with_max_cost].keys():
            max_replica_cost = 0
            tmp_workload_partition = None
            configurations = None

            tmp_workload_partition1 = copy.deepcopy(self.workload_partition)
            tmp_workload_partition1[replica_id_with_min_cost][q_id] = self.workload_size
            configurations1 = self._configurations(tmp_workload_partition1)
            _, _, new_replicas_cost1 = self._evaluation(configurations1)

            max_replica_cost1 = np.max(new_replicas_cost1)

            tmp_workload_partition2 = copy.deepcopy(self.workload_partition)
            tmp_workload_partition2[replica_id_with_min_cost][q_id] = self.workload_size
            tmp_workload_partition2[replica_id_with_max_cost].pop(q_id)
            configurations2 = self._configurations(tmp_workload_partition2)
            _, _, new_replicas_cost2 = self._evaluation(configurations2)

            max_replica_cost2 = np.max(new_replicas_cost2)

            if max_replica_cost1 < max_replica_cost2:
                max_replica_cost = max_replica_cost1
                tmp_workload_partition = tmp_workload_partition1
                configurations = configurations1
            else:
                max_replica_cost = max_replica_cost2
                tmp_workload_partition = tmp_workload_partition2
                configurations = configurations2

            if max_replica_cost < current_max_replica_cost:
                new_workload_partition = tmp_workload_partition
                current_max_replica_cost = max_replica_cost
                query_id = q_id
                new_configurations = configurations


        for i in range(len(self.queries)):
            q_id = i + 1
            max_replica_cost = 0
            tmp_workload_partition = copy.deepcopy(self.workload_partition)
            configurations = None
            if replica_id_with_max_cost in queries_best_n[i]:
                if q_id not in self.workload_partition[replica_id_with_max_cost].keys():
                    tmp_workload_partition[replica_id_with_max_cost][q_id] = self.workload_size
                    configurations = self._configurations(tmp_workload_partition)
                    _, _, new_replicas_cost = self._evaluation(configurations)
                    max_replica_cost = np.max(new_replicas_cost)

                    if max_replica_cost < current_max_replica_cost:
                        new_workload_partition = tmp_workload_partition
                        current_max_replica_cost = max_replica_cost
                        query_id = q_id
                        new_configurations = configurations

        if new_workload_partition is not None:
            print(query_id, new_workload_partition)
            self.workload_partition = new_workload_partition
            self.configurations = new_configurations
            _, _, replicas_cost = self._evaluation(new_configurations)
            print(replicas_cost)
            return 1

        return 0
