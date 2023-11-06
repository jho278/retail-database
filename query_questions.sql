-- Number of stores in each country
select country_code, count(country_code) as frequency
from dim_store_details
group by country_code
order by frequency desc;

-- Which locations have most stores max 7
select locality, count(locality) as frequency
from dim_store_details
group by locality
order by frequency desc
limit 7;

-- Total sales grouped by month
WITH SalesData AS (
    SELECT ot.product_quantity, pr.product_price, da.month, (pr.product_price * ot.product_quantity)::numeric AS sale
    FROM orders_table ot
    INNER JOIN dim_products pr ON ot.product_code = pr.product_code
    INNER JOIN dim_date_times da ON ot.date_uuid = da.date_uuid
)
SELECT round(SUM(sale),2) AS total_sale, month
FROM SalesData
GROUP BY month
order by total_sale desc
limit 6;

-- Number of sales from online and offline
select
	count(*) as number_of_sales,
	sum(product_quantity),
	case 
		when store_code = 'WEB-1388012W' then 'Web'
		else 'Offline'
	end as location
from orders_table
group by location

-- Percentage of sales from each type of store
SELECT 
    st.store_type, 
    ROUND(SUM(pr.product_price * ot.product_quantity)::numeric, 2) AS total_sales,
    ROUND((SUM(pr.product_price * ot.product_quantity)::numeric / SUM(SUM(pr.product_price * ot.product_quantity)::numeric) OVER ()) * 100, 2) AS percentage
FROM 
    orders_table ot
INNER JOIN dim_store_details st ON ot.store_code = st.store_code
INNER JOIN dim_products pr ON ot.product_code = pr.product_code
GROUP BY 
    st.store_type
ORDER BY 
    total_sales DESC;

-- which month in each year produced highest cost of sales?
select 
	round(sum(pr.product_price * ot.product_quantity)::numeric,2) as total_sales,
	dt.year,
	dt.month
from orders_table ot
inner join dim_date_times dt on ot.date_uuid = dt.date_uuid
inner join dim_products pr on ot.product_code = pr.product_code
group by dt.year,dt.month
order by total_sales desc
limit 10

-- Bonus: what about highest sale in each month for each year
WITH YearlySales AS (
    SELECT 
        round(sum(pr.product_price * ot.product_quantity)::numeric,2) as total_sales,
        dt.year,
        dt.month,
        ROW_NUMBER() OVER (PARTITION BY dt.year ORDER BY sum(pr.product_price * ot.product_quantity) DESC) as rn
    FROM 
        orders_table ot
    INNER JOIN dim_date_times dt ON ot.date_uuid = dt.date_uuid
    INNER JOIN dim_products pr ON ot.product_code = pr.product_code
    GROUP BY 
        dt.year, dt.month
)
SELECT 
    total_sales,
    year,
    month
FROM 
    YearlySales
WHERE 
    rn = 1
ORDER BY 
    year desc;

-- Total staff number per country
select sum(staff_numbers) as total_staff_numbers , country_code
from dim_store_details
group by country_code
order by total_staff_numbers desc

-- which german store type sells the most
select 
	round(sum(ot.product_quantity * pr.product_price)::numeric,2) as total_sales, 
	st.store_type,
	st.country_code
from orders_table ot
inner join dim_store_details st on ot.store_code = st.store_code
inner join dim_products pr on ot.product_code = pr.product_code
where st.country_code = 'DE'
group by st.store_type, st.country_code
order by total_sales desc

-- average time per sales grouped by year
SELECT 
    year,
    AVG(time_taken) AS average_time_taken
FROM 
    (
        SELECT 
            year,
            lead(datetime) OVER (ORDER BY datetime) - datetime AS time_taken
        FROM 
            (
                SELECT 
                    to_timestamp(dt.year ||'-'  ||dt.month ||'-' || dt.day||'-' || timestamp, 'YYYY-MM-DD HH24:MI:SS') as datetime,
                    dt.year
                FROM 
                    orders_table ot
                INNER JOIN dim_date_times dt ON ot.date_uuid = dt.date_uuid
            ) sub
    ) sub2
GROUP BY 
    year
ORDER BY 
    average_time_taken desc
limit 6;