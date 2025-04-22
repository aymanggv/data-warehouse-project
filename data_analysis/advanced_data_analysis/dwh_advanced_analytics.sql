-- CHANGE OVER TIME ANALYSIS

select 
year(order_date) as order_year, 
month(order_date) as order_month,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.facts_table
where order_date is not null
group by year(order_date), month(order_date)
order by year(order_date), month(order_date)

-- Below is cleaner version of above code
select 
datetrunc(month, order_date) as order_date, 
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.facts_table
where order_date is not null
group by datetrunc(month, order_date)
order by datetrunc(month, order_date)

-- Below is another version to get the month name instead of number
select 
format(order_date, 'yyyy-MMM') as order_date, 
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.facts_table
where order_date is not null
group by format(order_date, 'yyyy-MMM')
order by format(order_date, 'yyyy-MMM')