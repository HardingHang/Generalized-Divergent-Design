import copy

import numpy as np

from dbms.workload import *
from divergent_design.divergent_design import DivergentDesign


class ClusterMerge(DivergentDesign):
    def __init__(self, db_name, num_replicas, config):
        DivergentDesign.__init__(self, db_name, num_replicas, config=config)
        self.iterations = config['cluster_merge']['iterations']
        self.exp = config['cluster_merge']['exp']

        self.num_clusters = (2 ** self.exp) * self.num_replicas
        self.workload_cluster = [{} for i in range(self.num_clusters)]
        self.cluster_configs = []

    def run(self):
        self.db_connector.exec_fetch("SELECT * FROM hypopg_reset();")
        self._initialize()
        self._clustering()
        self._merging_by_cost()
        _, _, replicas_cost = self._evaluation(self.configurations)
        print("cluster&merge", replicas_cost)

    def _initialize(self):
        self._generator()

        for q in self.queries:
            for i in range(self.num_clusters):
                if np.random.randint(2):
                    self.workload_cluster[i][q.nr] = self.workload_size

        self.cluster_configs = self._configurations(self.workload_cluster)

    def _clustering(self):
        for i in range(self.iterations):
            new_workload_cluster = [{} for i in range(self.num_clusters)]
            for q in self.queries:
                query_cluster_costs = np.zeros(self.num_clusters)
                for j in range(self.num_clusters):
                    query_cluster_costs[j] = self.cost_evaluation.calculate_cost(Workload([q]), self.cluster_configs[j])

                min_cost_cluster = np.argmin(query_cluster_costs)
                new_workload_cluster[min_cost_cluster][q.nr] = self.workload_size

            self.workload_cluster = new_workload_cluster
            self.cluster_configs = self._configurations(self.workload_cluster)

    def _merging_by_size(self):
        workload_cluster = copy.deepcopy(self.workload_cluster)
        cluster_item_cnt = np.zeros(self.num_clusters)
        for i in range(self.num_clusters):
            cluster_item_cnt[i] = len(workload_cluster[i])

        indexes_sorted = np.argsort(cluster_item_cnt)

        for i in range(self.exp):
            new_workload_cluster = []
            new_num_cluster = 2 ** (self.exp - i - 1) * self.num_replicas
            for j in range(new_num_cluster):
                new_workload_cluster.append(
                    {**workload_cluster[indexes_sorted[j]], **workload_cluster[indexes_sorted[-1 - j]]})

            cluster_item_cnt = np.zeros(new_num_cluster)
            for k in range(new_num_cluster):
                cluster_item_cnt[i] = len(new_workload_cluster[i])

            indexes_sorted = np.argsort(cluster_item_cnt)
            workload_cluster = new_workload_cluster

        self.workload_partition = workload_cluster
        self.configurations = self._configurations(self.workload_partition)

    def _merging_by_cost(self):
        clusters_cost = np.zeros(self.num_clusters)

        for i in range(self.num_clusters):
            cluster_workload = self.workload_cluster[i]
            queries = []
            for q in self.queries:
                if q.nr in cluster_workload:
                    queries.extend([q] * cluster_workload[q.nr])
            clusters_cost[i] = self.cost_evaluation.calculate_cost(Workload(queries), self.cluster_configs[i])

        indexes_sorted = np.argsort(clusters_cost)

        for i in range(self.exp):
            workload_cluster = []
            new_num_cluster = 2 ** (self.exp - i - 1) * self.num_replicas
            for j in range(new_num_cluster):
                workload_cluster.append(
                    {**self.workload_cluster[indexes_sorted[j]], **self.workload_cluster[indexes_sorted[-1 - j]]})

            clusters_cost = np.zeros(new_num_cluster)

            self.cluster_configs = self._configurations(workload_cluster)

            for j in range(new_num_cluster):
                cluster_workload = workload_cluster[i]
                queries = []
                for q in self.queries:
                    if q.nr in cluster_workload:
                        queries.extend([q] * cluster_workload[q.nr])
                clusters_cost[i] = self.cost_evaluation.calculate_cost(Workload(queries), self.cluster_configs[i])

            indexes_sorted = np.argsort(clusters_cost)
            self.workload_cluster = workload_cluster
            print(self.workload_cluster)

        self.workload_partition = workload_cluster
        self.configurations = self._configurations(self.workload_partition)
