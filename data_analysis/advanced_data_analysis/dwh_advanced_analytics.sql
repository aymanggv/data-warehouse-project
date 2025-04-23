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


-- Find change in customers over the months
select 
datetrunc(month, order_date) as order_date, 
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
count(distinct customer_key) - lag(count(distinct customer_key)) over(order by datetrunc(month, order_date)) as customer_changes,
sum(quantity) as total_quantity
from gold.facts_table
where order_date is not null
group by datetrunc(month, order_date)
order by datetrunc(month, order_date)


-- Calculate the total sales per month and the running total of sales over time
select order_date,
total_sales,
sum (total_sales) over (order by order_date) as running_total_sales
from(
select 
datetrunc(month, order_date) as order_date, 
sum(sales_amount) as total_sales
from gold.facts_table
where order_date is not null
group by datetrunc(month, order_date)
) t


-- Calculate the total sales per month and the running total of sales over time but reset it after the end of the year
select order_date,
total_sales,
sum (total_sales) over (partition by datetrunc(year, order_date) order by order_date) as running_total_sales
from(
select 
datetrunc(month, order_date) as order_date, 
sum(sales_amount) as total_sales
from gold.facts_table
where order_date is not null
group by datetrunc(month, order_date)
) t


-- Calculate the total sales per month, the running total of sales over time and movin average of the price
select order_date,
total_sales,
sum (total_sales) over (order by order_date) as running_total_sales,
avg(avg_price) over (order by order_date) as moving_average
from(
select 
datetrunc(month, order_date) as order_date, 
sum(sales_amount) as total_sales,
avg (price) as avg_price
from gold.facts_table
where order_date is not null
group by datetrunc(month, order_date)
) t


-- Analyze the yearly performance of products by comparing each products sales to both its average sales performance and the previous years sales
with cte as(
select 
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.facts_table f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null
group by year(f.order_date), p.product_name
)
select order_year, 
product_name, 
current_sales, 
AVG(current_sales) over(partition by product_name) as avg_sales, 
current_sales - AVG(current_sales) over(partition by product_name) as diff_avg,
case when current_sales - AVG(current_sales) over(partition by product_name) > 0 then 'Above Average'
	 when current_sales - AVG(current_sales) over(partition by product_name) < 0 then 'Below Average'
	 else 'Average'
end as avg_change,
LAG(current_sales) over (partition by product_name order by order_year) as previous_year_sales,
current_sales - LAG(current_sales) over (partition by product_name order by order_year) as diff_yoy,
case when current_sales - LAG(current_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
	 when current_sales - LAG(current_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
	 else 'No Change'
end as avg_change
from cte
order by product_name, order_year
;



--Which categories contribute the most to overall sales?
with cte as
(
select 
p.category,
sum(f.sales_amount) as total_sales
from gold.facts_table f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null
group by p.category
)
select *, 
SUM(total_sales) over () as overall_sales,
concat(round((cast (total_sales as float) * 100.0 / sum(total_sales) over ()), 2), '%') AS percent_of_total
from cte
order by total_sales desc