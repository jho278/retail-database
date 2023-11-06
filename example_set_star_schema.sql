-- This calculates the length of int characters by casting it to text
select max(char_length(cast(product_code as TEXT))) as max_length
from orders_table

-- Altering the datatypes of existing table
alter table orders_table
	alter column date_uuid set data type uuid USING (date_uuid::uuid),
	alter column user_uuid set data type uuid USING (date_uuid::uuid),
	alter column card_number type VARCHAR(20),
	alter column store_code  type VARCHAR(15),
	alter column product_code type VARCHAR(15),
	alter column product_quantity type smallint

-- Altering specific records - requires where to be primary key
    UPDATE dim_store_details
    SET address = 'N/A', locality = 'N/A', latitude = 0, longitude =0
    WHERE index = 0

-- Replacing £ with ''
update dim_products
set product_price = replace(product_price, '£', '')

-- Adding category column based on existing column
update dim_products
SET weight_range_kg = 
    CASE
        WHEN weights < 2 THEN 'Light'
        WHEN weights BETWEEN 2 AND 39 THEN 'Mid_Sized'
        WHEN weights BETWEEN 40 AND 139 THEN 'Heavy'
        WHEN weights >= 140 THEN 'Truck_Required'
        ELSE 'N/A'
    END;

-- Separating the date column to year, month and day
ALTER TABLE dim_date_times 
ADD COLUMN year_column INTEGER,
ADD COLUMN month_column INTEGER,
ADD COLUMN day_column INTEGER;

update dim_date_times
set 
	year_column = DATE_PART('YEAR', date), 
    month_column = DATE_PART('MONTH', date), 
    day_column = DATE_PART('DAY', date);
