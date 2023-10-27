# %%
from database_utils import DatabaseConnector
import pandas as pd

class DataExtractor:
    def __init__(self,instance, table_name):
        self.db = instance
        self.table = table_name
    
    def read_rds_table(self):
        self.db.read_db_creds()
        self.db.init_db_engine()
        with self.db.engine.connect() as conn:
            db_table = pd.read_sql_table(self.table, conn)
            return db_table


# %%
if __name__ == '__main__':
    init = DatabaseConnector()
    test = DataExtractor(init, 'legacy_users')
    table = test.read_rds_table()
    print(table)


# %%
