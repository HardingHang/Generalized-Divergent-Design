class EncodingAlgorithm:
    def __init__(self, tables, columns):
        self.representation = None
        self.table_names = []
        self.column_names = []
        for c in columns:
            self.column_names.append(c.name)
        for t in tables:
            self.table_names.append(t.name)

    def encode(self, query):
        self.representation = self._encode(query)
        return self.representation

    def _encode(self, query):
        raise NotImplementedError("_encode(self, " "query) missing")
