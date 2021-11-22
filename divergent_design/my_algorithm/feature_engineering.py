import numpy as np
import psqlparse

from .encoding_algorithm import EncodingAlgorithm


class FeatureEngineering(EncodingAlgorithm):
    def __init__(self, tables, columns):
        EncodingAlgorithm.__init__(self, tables, columns)
        self.feature_vector = None

        self.num_columns = len(self.column_names)
        self.num_tables = len(self.table_names)

    def _encode(self, query):
        self.feature_vector = None
        self.feature_list = []
        # [ [table_ref], [select_columns], [filter_columns], [order_columns], [groupby_columns], [join_columns]]

        table_vector = np.zeros(self.num_tables, dtype=np.int8)
        self.feature_list.append(table_vector)
        for i in range(5):
            self.feature_list.append(np.zeros(self.num_columns, dtype=np.int8))

        self.statements = psqlparse.parse(query)
        for statement in self.statements:
            if statement.type == 'SelectStmt':
                self._featurization(statement._obj)
        self.feature_vector = np.hstack(self.feature_list)
        return self.feature_vector

    def _featurization(self, select_statement):
        from_cluase = select_statement.get('fromClause')
        where_clause = select_statement.get('whereClause')
        targets = select_statement.get('targetList')
        sort_clause = select_statement.get('sortClause')
        group_clause = select_statement.get('groupClause')

        self._table_ref(from_cluase)
        self._select_column_ref(targets)
        self._where_column_ref(where_clause)
        self._sort_column_ref(sort_clause)
        self._group_column_ref(group_clause)

    def _table_ref(self, from_clause):
        if from_clause is None:
            return
        for i in from_clause:
            val = i.get('RangeVar')
            if val is not None:
                table_ref = val.get('relname')
                if table_ref in self.table_names:
                    self.feature_list[0][self.table_names.index(table_ref)] = 1
            val = i.get('RangeSubselect')
            if val is not None:
                subquery = val.get('subquery')
                self._featurization(subquery.get('SelectStmt'))

    def _select_column_ref(self, targets):
        if targets is None:
            return
        for target in targets:
            val = self._dict_get(target, 'ColumnRef')
            if val is not None:
                column_ref = self._column_ref({'ColumnRef': val})
                if column_ref in self.column_names:
                    self.feature_list[1][self.column_names.index(column_ref)] = 1

    def _where_column_ref(self, where_clause):
        if where_clause is None:
            return
        for k, v in where_clause.items():
            if k == 'A_Expr':
                self._a_expr(v)
            elif k == 'BoolExpr':
                self._bool_expr(v)

    def _bool_expr(self, bool_expr):
        args = bool_expr.get('args')
        for arg in args:
            for k, v in arg.items():
                if k == 'A_Expr':
                    self._a_expr(v)
                elif k == 'BoolExpr':
                    self._bool_expr(v)
                elif k == 'SubLink':
                    subselect = v.get('subselect')
                    if subselect is not None:
                        self._featurization(subselect.get('SelectStmt'))

    def _a_expr(self, a_expr):
        rexpr = a_expr.get('rexpr')
        if type(rexpr) != dict:
            column_ref = self._column_ref(a_expr.get('lexpr'))
            if column_ref in self.column_names:
                self.feature_list[2][self.column_names.index(column_ref)] = 1
            return

        if rexpr.get('ColumnRef') is not None:
            column_ref = self._column_ref(a_expr.get('lexpr'))
            if column_ref in self.column_names:
                self.feature_list[5][self.column_names.index(column_ref)] = 1
            column_ref = self._column_ref(rexpr)
            if column_ref in self.column_names:
                self.feature_list[5][self.column_names.index(column_ref)] = 1
        else:
            column_ref = self._column_ref(a_expr.get('lexpr'))
            if column_ref in self.column_names:
                self.feature_list[2][self.column_names.index(column_ref)] = 1

            sublink = rexpr.get("SubLink")
            if sublink is not None:
                subselect = sublink.get('subselect')
                if subselect is not None:
                    self._featurization(subselect.get('SelectStmt'))

    def _sort_column_ref(self, sort_clause):
        if sort_clause is None:
            return
        for sortby in sort_clause:
            column_ref = self._column_ref(sortby.get('SortBy').get('node'))
            if column_ref in self.column_names:
                self.feature_list[3][self.column_names.index(column_ref)] = 1

    def _group_column_ref(self, group_clause):
        if group_clause is None:
            return
        for group_column_ref in group_clause:
            column_ref = self._column_ref(group_column_ref)
            if column_ref in self.column_names:
                self.feature_list[4][self.column_names.index(column_ref)] = 1

    def _column_ref(self, column_ref_dict):
        """
        Get column name from the ColumnRef Dict
        :return: column name
        """
        column_ref = column_ref_dict.get('ColumnRef')
        if column_ref is None:
            return None
        column_name = column_ref.get('fields')[0].get('String')
        if column_name is not None:
            column_name = column_name.get('str')
        else:
            return None
        return column_name

    def _dict_get(self, dict, key, default=None):
        tmp = dict
        for k, v in tmp.items():
            if k == key:
                return v
            else:
                if type(v) == type({}):
                    ret = self._dict_get(v, key)
                    if ret is not default:
                        return ret
        return default
