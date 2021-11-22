import random

import numpy as np

from divergent_design.divergent_design import DivergentDesign


class DIVGDESIGN(DivergentDesign):
    def __init__(self, db_name, num_replicas, config):
        DivergentDesign.__init__(self, db_name, num_replicas, config=config)

    def run(self):
        self._initialize()
        self._update()

        _, _, replicas_cost = self._evaluation(self.configurations)
        return replicas_cost

    def _initialize(self):
        self._generator()

        all_replicas = [i for i in range(self.num_replicas)]
        for q in self.queries:
            replicas = random.sample(all_replicas, self.factor_n)
            for i in replicas:
                self.workload_partition[i][q.nr] = self.workload_size // self.factor_n

        self.configurations = self._configurations(self.workload_partition)

    def _update(self):
        max_iter = 10
        min_rate = 0.01
        iter_cnt = 1
        cost_rate = 1
        queries_replicas_cost, queries_best_n, replicas_cost = self._evaluation(self.configurations)

        while iter_cnt <= 10 and cost_rate >= 0.01:
            new_workload_partition = [{} for i in range(self.num_replicas)]
            total_cost = np.sum(replicas_cost)
            for i in range(len(self.queries)):
                for j in queries_best_n[i][0:self.factor_n]:
                    new_workload_partition[j][self.queries[i].nr] = self.workload_size // self.factor_n
            new_configurations = self._configurations(new_workload_partition)
            queries_replicas_cost, queries_best_n, replicas_cost = self._evaluation(new_configurations)
            new_total_cost = np.sum(replicas_cost)
            cost_rate = (total_cost - new_total_cost) / total_cost

            iter_cnt += 1

            if iter_cnt > 10 or cost_rate < 0.01:
                break
            else:
                self.workload_partition = new_workload_partition
                self.configurations = new_configurations
