# %%
import pandas as pd
import numpy as np
from dateutil.parser import parse
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:
    def __init__(self,df):
        self.df = df
    
    def try_parsing_date(self,text):
        try:
            return parse(text)
        except Exception:
            return pd.NaT
    
    def clean_date_columns(self):
        for col in self.df.columns:  
            if 'date' in col:
                self.df[col] = self.df[col].apply(self.try_parsing_date)
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
        na_index = self.df.index[self.df['date_of_birth'].isna()].tolist() # All records corresponding to this is random data
        self.df = self.df.drop(na_index)
        return self.df
    
    def clean_phone_number(self):
        us_indexes = self.df[self.df['country'] == "United States"].index
        other_indexes = self.df[self.df['country'] != "United States"].index

        self.df.loc[us_indexes, 'phone_number'] = self.df.loc[us_indexes, 'phone_number'].str.replace(r'(\+\d{1}|x\d+|001-|[\.\-\(\)])', '', regex=True)
        self.df.loc[us_indexes, 'phone_number'] = self.df.loc[us_indexes, 'phone_number'].apply(lambda x: np.nan if len(str(x)) != 11 else x)

        self.df.loc[other_indexes, 'phone_number'] = self.df.loc[other_indexes, 'phone_number'].str.replace(r'(\+\d{2})|\s+|[(|)]', '', regex=True)
        self.df.loc[other_indexes, 'phone_number'] = self.df.loc[other_indexes, 'phone_number'].apply(lambda x: np.nan if len(str(x)) != 11 else x)
        return self.df
    
    def clean_country_code(self):
        self.df.loc[df['country_code'] == "GGB", 'country_code']= df.loc[df['country_code'] == "GGB", 'country_code'] = "GB"
        return self.df
    
    def clean_address(self):
        self.df['address'] = self.df['address'].str.replace('\n', ',')
        return self.df
    
    def clean_user_data(self):
        self.df = self.clean_date_columns()
        self.df = self.clean_phone_number()
        self.df = self.clean_country_code()
        self.df = self.clean_address()
        self.df = self.df.drop('index', axis = 1)
        return self.df  

# %%
if __name__ == '__main__':
    db = DatabaseConnector()
    table = DataExtractor(db, 'legacy_users')
    df = table.read_rds_table()
    clean_data = DataCleaning(df)
    print(df)

# %%
cleaned_data = clean_data.clean_user_data()
print(cleaned_data)

# %%
db.upload_to_db(cleaned_data,'dim_users')
# %%
print(cleaned_data['country_code'].value_counts())
print(cleaned_data.info())
# %%
remove_dummy = clean_data.clean_date_columns()
print(remove_dummy)
print(remove_dummy.info())

# %%
remove_na_phone = clean_data.clean_phone_number()
            

# %%
print(remove_na_phone)
print(remove_na_phone.info())

# %%
