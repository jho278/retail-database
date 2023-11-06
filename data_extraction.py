# %%
import pandas as pd
import requests
import tabula
import boto3
from io import BytesIO

# %%
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
        self.pdf_list = tabula.read_pdf(link, pages='all', stream = False)
        self.pdf_df = pd.concat(self.pdf_list, ignore_index=True)
        return self.pdf_df
    
    def list_number_of_stores(self,endpoint,header:dict):
        self.endpoint = endpoint
        self.header = header

        response = requests.get(self.endpoint, headers = self.header)

        if response.status_code == 200:
            self.data = response.json()
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
        return self.data
    
    def retrieve_stores_data(self,endpoint, header):
        self.endpoint = endpoint
        self.headers = header
        data_list = []
        for store_number in range(1, 452):
            response = requests.get(f"{self.endpoint}/{store_number}", headers=self.header)

            if response.status_code == 200:
                data = response.json()
                data_list.append(data)
                # Process the data as needed for each store
                print(f"Data for store {store_number}: {data}")
            else:
                print(f"Request for store {store_number} failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")

        self.df_api = pd.DataFrame(data_list)
        return self.df_api
    
    def extract_from_s3(self,link):
        # Example S3 path
        s3_path = link

        # Separating bucket and object
        if s3_path.startswith("s3://"):
            path_without_protocol = s3_path[len("s3://"):]
            self.bucket_name, self.object_key = path_without_protocol.rsplit('/', 1)
        else:
            print("Invalid S3 path.")

        # Initialize Boto 3 S3 client
        self.s3 = boto3.client('s3')

        # Read the file from S3
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=self.object_key)
            # Access and read the content of the file. Data type = Bytes due to get request
            data = response['Body'].read()

            # Process the data as needed
            self.df = pd.read_csv(BytesIO(data), index_col = 0)  # Replace this with your processing logic
            return self.df
        
        except Exception as e:
            print(f"Error: {e}")

    def extract_json_s3(self,link): 
        response = requests.get(link)
        if response.status_code == 200:
            self.data = response.json()
            self.dict_df = pd.DataFrame(self.data)
            return self.dict_df
        else:
            print("Failed to retrieve data. Status code:", response.status_code)   
        

# %%
if __name__ == '__main__':
    from database_utils import DatabaseConnector
    init = DatabaseConnector()
    test = DataExtractor()
    table = test.read_rds_table(init, 'legacy_users')
    print(table)

    pdf = test.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    print(pdf.head(3))

# %%
if __name__ == '__main__':
    get_creds = DatabaseConnector()
    creds = get_creds.read_db_creds()
    retrieve_store = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
    number_store = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    init = DataExtractor()
    test = init.list_number_of_stores(number_store,creds['header'])
# %%
    print(test['number_stores'])
    # %%
    test2 = init.retrieve_stores_data(retrieve_store,creds['header'])
    # %%
    test2.to_csv("api_data.csv",index=True)
    # %%
    print(test2)

# %%
if __name__ == '__main__':
    get_s3_object = DataExtractor()
    s3_object = get_s3_object.extract_from_s3("s3://data-handling-public/products.csv")
    print(s3_object)
# %%
