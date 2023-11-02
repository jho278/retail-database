# %%
import yaml
from sqlalchemy import create_engine
from sqlalchemy import text
from data_extraction import DataExtractor

# %%
class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        with open('db_creds.yaml','r') as file:
            self.credentials = yaml.safe_load(file)
        return self.credentials
    
    def init_db_engine(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = self.credentials['RDS_HOST']
        USER = self.credentials['RDS_USER']
        PASSWORD = self.credentials['RDS_PASSWORD']
        DATABASE = self.credentials['RDS_DATABASE']
        PORT = self.credentials['RDS_PORT']
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return self.engine
    
    
    def list_db_tables(self):
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            result = conn.execute(query)
            tables = result.fetchall()
            for table in tables:
                print(table[0])
    
    def upload_to_db(self,df,table_name):
        self.credentials = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = self.credentials['SQL_HOST']
        USER = self.credentials['SQL_USER']
        PASSWORD = self.credentials['SQL_PASSWORD']
        DATABASE = self.credentials['SQL_DATABASE']
        PORT = self.credentials['SQL_PORT']
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn: 
            df.to_sql(table_name,conn,if_exists='replace')

# %%

if __name__ == "__main__":
    import pandas as pd
    from sqlalchemy import create_engine

    test = DatabaseConnector()
    test.read_db_creds()
    test.init_db_engine()
    test.list_db_tables()

    DATABASE_TYPE = 'postgresql'
    DBAPI = 'psycopg2'
    HOST = test.credentials['SQL_HOST']
    USER = test.credentials['SQL_USER']
    PASSWORD = test.credentials['SQL_PASSWORD']
    DATABASE = test.credentials['SQL_DATABASE']
    PORT = test.credentials['SQL_PORT']
    engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    
    with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn: 
        users = pd.read_sql_table('dim_users',conn)
        print(users)
        
    test.credentials['SQL_HOST']


# %% 
# Extract orders_table data from AWS RDS
if __name__ == "__main__":
    test = DatabaseConnector()
    test.read_db_creds()
    test.init_db_engine()
    test.list_db_tables()
# %%
if __name__ == "__main__":
    extract = DataExtractor()
    table = extract.read_rds_table(test, 'orders_table')
    print(table)
# %%
# %%
