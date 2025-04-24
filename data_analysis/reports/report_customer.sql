/*
=============================================================================
Customer Report
=============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors
Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
    2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
       - total orders
       - total sales
       - total quantity purchased
       - total products
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last order)
       - average order value
       - average monthly spend
=============================================================================
*/

/*1. Base query - Retrieve core columns from table*/
IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    DROP VIEW gold.report_customers;
GO
create view gold.report_customers as
with base_query as(
SELECT
    f.order_number,
    f.product_key,
    f.order_date,
    f.sales_amount,
    f.quantity,
    c.customer_key,
    c.customer_number,
	CONCAT(c.first_name, ' ', c.last_name) as customer_name,
    c.birthdate,
	DATEDIFF(year, c.birthdate, GETDATE()) as age
FROM gold.facts_table f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
where order_date is not null
), 
/* Customer Aggregations: Summarizes key metrics at customer level*/
customer_aggregations as (
select
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count (distinct product_key) as total_products,
MAX(order_date) as last_order_date,
DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) as lifespan
from base_query 
group by 
customer_key,
customer_number,
customer_name,
age
)
select
customer_key,
customer_number,
customer_name,
age,
case when age < 20 then 'Under 20'
	 when age between 20 and 29 then '20-29'
	 when age between 30 and 39 then '30-39'
	 when age between 40 and 49 then '40-49'
	 when age >=50 then '50 and above'
	 else 'N/A'
end as age_group,
case when total_sales > 5000 and lifespan >= 12 then 'VIP'
	 when total_sales < 5000 and lifespan >= 12 then 'Regular'
	 when lifespan <= 12 then 'New'
	 else 'N/A'
end as customer_segment,
last_order_date,
DATEDIFF(month, last_order_date, GETDATE()) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan,
case when total_orders = 0 then 0
	 else total_sales / total_orders 
end as avg_order_value,
case when lifespan = 0 then total_sales -- We keep the value as total sales as the customer has been here for a month so by calcualtion the avg would still be total sales
	 else total_sales / lifespan 
end as avg_monthly_spending
from customer_aggregations
