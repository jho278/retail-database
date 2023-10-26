# %%
import yaml
from sqlalchemy import create_engine
from sqlalchemy import text


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
        engine = self.init_db_engine()
        df.to_sql(table_name, engine, if_exists = 'replace')
    
# %%

if __name__ == "__main__":
    test = DatabaseConnector()
    creds = test.read_db_creds()
    print(creds)
    db_engine = test.init_db_engine()
    db_engine.connect()
    test.list_db_tables()


# %%
engine.execution_options(isolation_level='AUTOCOMMIT').connect()
# %%