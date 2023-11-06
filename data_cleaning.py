# %%
import pandas as pd
from dateutil.parser import parse

# %%
class DataCleaning:
    """
    This class contains methods that will clean a wide range of data sources from APIs, csv files and AWS S3 buckets.

    The first set of methods are aimed at cleaning a table pulled from AWS RDS.
    The second set of methods clean data from a csv file pulled from the S3 bucket, ultimately being combined in the method clean_card_data
    The last set of methods will clean data pulled from an API

    The purpose of these functions is to check for correct formatting for each column in the data, if it incorrect then the data is changed to '0' or NaT 
    """
    def __init__(self,df):
        self.df = df
    
    def try_parsing_date(self,text):
        """
        This method is a supporting function used in clean_date_columns. It tries to parse the date data and if not possible then it is safe to assume it is not the correct format so returns NaT
        """
        try:
            return parse(text)
        except Exception:
            return pd.NaT
    
    def clean_date_columns(self):
        """
        This method will drop any rows of data that does not have date data in the correct format. This is because there are randomly generated values for non-date records.
        """
        for col in self.df.columns:  
            if 'date' in col:
                self.df[col] = self.df[col].apply(self.try_parsing_date)
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
        na_index = self.df.index[self.df['date_of_birth'].isna()].tolist() # All records corresponding to this is random generated data
        self.df = self.df.drop(na_index)
        return self.df
    
    def clean_phone_number(self):
        """
        Phone numbers from the US or Europe have a specific number of digits for phone numbers, this method evaluates the phone number column and returns 0 for incorrectly formatted phone numbers
        
        Phone numbers in this case are treated as string type, easier to evaluate as string and not used with int functions
        """
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
        """
        The above methods are combined into a singular method to tidy the users table
        """
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
        self.df.drop(['expected_num_dig','comparison'], axis = 1, inplace = True)
        return self.df
    
    def remove_question_mark(self):
        self.df['card_number'] = self.df['card_number'].astype(str).str.replace('?', '')
        return self.df

    def remove_str_null(self):
        str_null = pdf.loc[pdf['card_number'] == 'NULL'].index.tolist()
        self.df.drop(str_null, axis = 0, inplace = True)
        return self.df

    def clean_card_data(self):
        self.df = self.remove_question_mark()
        self.df = self.clean_card_provider()
        #self.df = self.clean_card_number()
        return self.df

    def remove_random_generated(self):
        self.df['country_code'] = self.df['country_code'].astype(str)
        self.df = self.df[self.df['country_code'].str.len() <= 2]
        return self.df
    
    def clean_api_address(self):
        self.df['address'] = self.df['address'].str.replace('\n', ',')
        return self.df
    
    def clean_api_date(self):
        for col in self.df.columns:  
            if 'date' in col:
                 self.df[col] = pd.to_datetime(self.df[col], format = 'mixed')
        return self.df
    
    def clean_staff_numbers(self):
        self.df['staff_numbers'] = self.df['staff_numbers'].replace(r'\D', '', regex=True)
        return self.df
    
    def convert_lat_lon_type(self):
        self.df['latitude'] = self.df['latitude'].astype(float)
        self.df['longitude'] = self.df['longitude'].astype(float)
        return self.df
    
    def clean_continent(self):
        self.df['continent'] = self.df['continent'].str.replace('ee', '')
        return self.df

    def clean_store_data(self):
        """
        The above methods are combined to tidy the store data
        """
        self.df = self.df.drop(['Unnamed: 0','lat'],axis = 1)
        self.df = self.remove_random_generated()
        self.df = self.clean_api_address()
        self.df = self.clean_api_date()
        self.df = self.clean_staff_numbers()
        self.df = self.convert_lat_lon_type()
        self.df = self.clean_continent()
        return self.df

    def transform_weight(self,row):
        weight, unit = row['weights'],row['units']
        if unit == 'kg':
            return weight
        elif unit == 'g'or unit == 'g .':
            return weight / 1000 
        elif unit == 'ml':
            return weight / 1000
        elif unit == 'oz':
            return weight * 0.0283495
        else:
            return 0
    
    def convert_product_weights(self):
        self.df[['weights','units']] = self.df['weight'].str.extract('(\d*\.?\d+)(\D+)', expand=True)
        self.df['weights'] = self.df['weights'].astype(float)
        self.df['weights'] = self.df.apply(self.transform_weight, axis = 1)
        self.df.drop(['weight','units'], axis=1, inplace = True)
        return self.df
    
    def remove_null(self):
        self.df.dropna(inplace = True)
        return self.df
    
    def clean_category(self):
        self.cat = self.df['category'].value_counts()
        self.get_cat = self.cat[self.cat < 2].index.tolist()
        self.df = self.df[~self.df['category'].isin(self.get_cat)]
        return self.df
        
    def clean_product_data(self):
        """
        The above methods are combined to tidy the products table
        """
        self.df = self.remove_null()
        self.df = self.clean_category()
        self.df = self.convert_product_weights()
        self.df = self.clean_api_date()
        return self.df

    def clean_orders_data(self):
        self.df.drop(['first_name','last_name','1','level_0','index'], axis = 1, inplace = True)
        return self.df
    
    def concatenate_date(self):
        day = self.df['day']
        month = self.df['month']
        year = self.df['year']
        time = self.df['timestamp']
        self.df['date'] = pd.to_datetime(year.astype(str) + '-' + month.astype(str) + '-' + day.astype(str) + ' ' + time, format = 'mixed', errors = 'coerce')
        self.df.drop(['day','month','year','timestamp'], axis = 1, inplace = True)
        return self.df
    
    def remove_null_json(self):
        null_df = self.df.index[self.df.isnull().any(axis=1)]
        self.df.drop(null_df, inplace = True)
        return self.df
    
    def clean_json_s3(self):
        self.df = self.concatenate_date()
        self.df = self.remove_null_json()
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
    print(cleaned_data.info())
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
    cleaned_data.head(10)

# %%
if __name__ == '__main__':
    from data_extraction import DataExtractor
    from database_utils import DatabaseConnector
    init = DatabaseConnector()
    test = DataExtractor()
    pdf = test.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    print(pdf.info())

    # %%
    tidy_card = DataCleaning(pdf)
    tidied_card = tidy_card.clean_card_data()
    print(tidied_card)
    # %%
    print(tidied_card.loc[tidied_card['card_number'] == '4971858637664481'])
    #%%
    init.upload_to_db(tidied_card,'dim_card_details')

    # %%
    tidied_card[tidied_card['comparison']==False].head(20)
    # %%
    result = tidied_card[tidied_card['card_number'].str.contains(r'\D')]
    print(result)
    # %%
    type(tidied_card['card_number'][1])
# %%
# Cleaning the API data on store details
if __name__ == '__main__':
    import pandas as pd
    from database_utils import DatabaseConnector
    df = pd.read_csv("api_data.csv")
    cleaner = DataCleaning(df)
    df_clean = cleaner.clean_store_data()
    upload = DatabaseConnector()
    upload.upload_to_db(df_clean, 'dim_store_details')

# %%
if __name__ == '__main__':
    #df_clean = cleaner.remove_random_generated()
    #df_clean = cleaner.clean_api_address()
    df_clean = cleaner.clean_api_date()
    print(df_clean.head(20))
if __name__ == '__main__':
    
    upload = DatabaseConnector()
    upload.upload_to_db(df_clean, 'dim_store_details')



# %%
if __name__ == '__main__':
    get_s3_object = DataExtractor()
    s3_object = get_s3_object.extract_from_s3("s3://data-handling-public/products.csv")
    get_cleaning = DataCleaning(s3_object)
    print(s3_object)
# %%
# %%
if __name__ == '__main__':
    print(type(test))
    print(type(s3_object))

# %%
if __name__ == '__main__':
    test = get_cleaning.clean_category()
    test = get_cleaning.remove_null()
    test = get_cleaning.convert_product_weights()
    print(test['category'].value_counts())
    print(test)
# %%
if __name__ == '__main__':
    test = get_cleaning.clean_product_data()
# %%
if __name__ == '__main__':
    print(test)
    print(test['date_added'].isnull().sum())

# %%
if __name__ == '__main__':
    upload = DatabaseConnector()
    upload.upload_to_db(test, 'dim_products')
# %%
if __name__ == '__main__':
    from data_extraction import DataExtractor
    from database_utils import DatabaseConnector
    from data_cleaning import DataCleaning
    init = DatabaseConnector()
    test = DataExtractor()
    df = test.read_rds_table(init, 'orders_table')

    print(df.info())
# %%
if __name__ == '__main__':
    clean = DataCleaning(df)
    cleaned = clean.clean_orders_data()
    cleaned.info()
    cleaned.loc[cleaned['user_uuid'] == '0423a395-a04d-4e4a-bd0f-d237cbd5a295']
    add_db = cleaned[['date_uuid','user_uuid']]
    add_db.head(5)# %%
    init.upload_to_db(add_db,'inner_join')

