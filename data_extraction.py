# %%
import pandas as pd
import tabula

class DataExtractor:
    def __init__(self):
        pass
    
    def read_rds_table(self,instance, table_name):
        self.db = instance
        self.table = table_name

        self.db.read_db_creds()
        self.db.init_db_engine()
        with self.db.engine.connect() as conn:
            self.db_table = pd.read_sql_table(self.table, conn)
            return self.db_table
        
    def retrieve_pdf_data(self,link):
        self.pdf_list = tabula.read_pdf(link, pages='all')
        self.pdf_df = pd.concat(self.pdf_list, ignore_index=True)
        return self.pdf_df


# %%
if __name__ == '__main__':
    init = DatabaseConnector()
    test = DataExtractor()
    table = test.read_rds_table(init, 'legacy_users')
    print(table)

    pdf = test.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    print(pdf.head(3))

# %%
