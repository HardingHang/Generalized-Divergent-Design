import re

from .cost_evaluation import CostEvaluation
from dbms.cost_evaluation import CostEvaluation
from dbms.index_selection import IndexSelection
from dbms.postgres_connector import PostgresDatabaseConnector
from dbms.query_generator import QueryGenerator
from dbms.table_generator import TableGenerator
from dbms.workload import *
from divergent_design.my_algorithm.query_encoder import QueryEncoder


class MVSelectionAlgorithm:
    def __init__(self, database_connector):
        generating_connector = PostgresDatabaseConnector(None, autocommit=True)
        self.table_generator = TableGenerator(
            "tpch", 0.001, generating_connector, explicit_database_name="test_mv_tpch",
        )
        self.encoder = QueryEncoder(self.table_generator.tables, self.table_generator.columns)

        self.tables = self.encoder.encoder.table_names
        self.columns = self.encoder.encoder.column_names

        self.database_connector = database_connector
        self.database_connector.drop_indexes()
        self.cost_evaluation = CostEvaluation(database_connector, cost_estimation='runtime')

    def syntactically_relevant_mv(self, query):
        self.encoder.encode(query.text)
        feature_list = self.encoder.encoder.feature_list
        table_ref = []
        for idx in range(len(feature_list[0])):
            if feature_list[0][idx] == 1:
                table_ref.append(self.table_generator.tables[idx])
        projection_columns = []
        for idx in range(len(feature_list[1])):
            if feature_list[1][idx] == 1:
                projection_columns.append(self.table_generator.columns[idx])
        selection_columns = []
        for idx in range(len(feature_list[2])):
            if feature_list[2][idx] == 1:
                projection_columns.append(self.table_generator.columns[idx])
        group_columns = []
        for idx in range(len(feature_list[4])):
            if feature_list[4][idx] == 1:
                group_columns.append(self.table_generator.columns[idx])
        materialized_views = []
        for t in table_ref:
            mv = [[], [], [], []]  # [[table], [projection_columns]]
            for c in projection_columns:
                pass
            for c in group_columns:
                pass
