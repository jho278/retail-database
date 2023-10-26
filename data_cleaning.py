# %%
import pandas as pd
import numpy as np
from dateutil.parser import parse
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:
    def __init__(self,df):
        self.df = df
    
    def clean_user_data(self):
        pass
    
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
        self.df = self.df.drop('index', axis = 1)
        na_index = self.df.index[self.df['date_of_birth'].isna()].tolist() # All records corresponding to this is random data
        self.df = self.df.drop(na_index)
        return self.df
    
    def clean_phone_number(self):
        for idx,country in enumerate(self.df['country']):
            if country == "United States":
                self.df.loc[idx,'phone_number'] = self.df[idx,'phone_number'].str.replace(r'(\+\d{1}|x\d+|001-|[\.\-\(\)])','',regex=True)
                self.df.loc[~self.df[idx,'phone_number'].apply(lambda x: len(str(x)) != 10), 'phone_number'] = np.nan
            else:
                self.df.loc[idx,'phone_number'] = self.df[idx,'phone_number'].str.replace(r'(\+\d{2})|\s+|[(|)]','', regex= True)
                self.df.loc[~self.df[idx,'phone_number'].apply(lambda x: len(str(x)) != 11), 'phone_number'] = np.nan
        return self.df
    
    
        

# %%
if __name__ == '__main__':
    db = DatabaseConnector()
    table = DataExtractor(db, 'legacy_users')
    df = table.read_rds_table()

# %%
print(df)
#%%
clean_data = DataCleaning(df)
remove_dummy = clean_data.clean_date_columns()
print(remove_dummy)
print(remove_dummy.info())

# %%
remove_na_phone = clean_data.clean_phone_number()
            

# %%
print(remove_na_phone)
print(remove_na_phone.info())

# %%
