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
        self.df.loc[us_indexes, 'phone_number'] = self.df.loc[us_indexes, 'phone_number'].apply(lambda x: '0' if len(str(x)) != 11 else x)

        self.df.loc[other_indexes, 'phone_number'] = self.df.loc[other_indexes, 'phone_number'].str.replace(r'(\+\d{2})|\s+|[(|)]', '', regex=True)
        self.df.loc[other_indexes, 'phone_number'] = self.df.loc[other_indexes, 'phone_number'].apply(lambda x: '0' if len(str(x)) != 11 else x)
        
        self.df.loc[:,'phone_number'] = self.df.loc[:,'phone_number'].astype(str)
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
    
    def clean_card_provider(self):
        card_names = self.df['card_provider'].value_counts()
        get_card_names = card_names[card_names > 1000].index.tolist()
        self.df = self.df[self.df['card_provider'].isin(get_card_names)]
        return self.df
    
    def get_card_length(self,column):
        if 'Diners' in column:
            return 14
        elif 'American Express' in column:
            return 15
        elif 'Discover' in column:
            return 16
        elif 'Mastercard' in column:
            return 16
        elif 'JCB' in column or 'VISA' in column:
            return int(''.join(filter(str.isdigit, column)))
        elif 'Maestro' in column:
            return list(range(12,20))
        elif 'Diners Club / Carte Blanche' in column:
            return 14
        else:
            return 0
    
    def clean_card_number(self): 
        self.df['card_number'] = self.df['card_number'].astype(str)
        self.df.loc[:,'expected_num_dig'] = self.df['card_provider'].apply(self.get_card_length)
        self.df.loc[:,'comparison'] = self.df.apply(lambda row: len(str(row['card_number'])) == row['expected_num_dig'] if isinstance(row['expected_num_dig'], int) else len(str(row['card_number'])) in row['expected_num_dig'], axis=1)
        self.df.loc[self.df['comparison'] == False, 'card_number'] = '0'
        self.df.loc[self.df['card_number'].str.contains(r'\D'), 'card_number'] = '0'
        return self.df
    
    def clean_card_data(self):
        self.df = self.clean_card_provider()
        self.df = self.clean_card_number()
        return self.df

# %%
if __name__ == '__main__':
    db = DatabaseConnector()
    table = DataExtractor()
    df = table.read_rds_table(db, 'legacy_users')
    clean_data = DataCleaning(df)
    print(df)

# %%
if __name__ == '__main__':
    cleaned_data = clean_data.clean_user_data()
    print(cleaned_data)
#%%   
if __name__ == '__main__':
    print(type(cleaned_data['phone_number'][0]))
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
if __name__ == '__main__':
    init = DatabaseConnector()
    test = DataExtractor()
    pdf = test.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    print(pdf.head(2))

    # %%
    tidy_card = DataCleaning(pdf)
    tidied_card = tidy_card.clean_card_data()
    #%%
    print(tidied_card)

    # %%
    tidied_card[tidied_card['comparison']==False].head(20)
    # %%
    result = tidied_card[tidied_card['card_number'].str.contains(r'\D')]
    print(result)
    # %%
    type(tidied_card['card_number'][1])
    # %%
