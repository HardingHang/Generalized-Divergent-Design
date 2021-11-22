from .feature_engineering import FeatureEngineering

ALGORITHMS = {
    "feature_engineering": FeatureEngineering
}


class QueryEncoder:
    def __init__(self, tables, columns, algorithm='feature_engineering'):
        self.encoder = ALGORITHMS[algorithm](tables, columns)

    def encode(self, query):
        return self.encoder.encode(query)
        a = 4
