--Import tabel clean ke SQL

if object_id('sales.C_Customers' , 'U') is not null
	drop table sales.C_Customers;
Create table sales.C_Customers(
customer_id nvarchar(50),
customer_zip_code_prefix int,
customer_city nvarchar(50),
customer_state nvarchar(50)
);

if object_id('sales.C_Order_Items' , 'U') is not null
	drop table sales.C_Order_Items;
Create table sales.C_Order_Items(
order_id nvarchar(50),
order_item_id int,
product_id nvarchar(50),
seller_id nvarchar(50),
price float,
shipping_charges float
);

if object_id('sales.C_Orders' , 'U') is not null
	drop table sales.C_Orders;
Create table sales.C_Orders(
order_id nvarchar(50),
customer_id nvarchar(50),
order_status nvarchar(50),
order_purchase_timestamp datetime,
order_approved_at datetime,
order_delivered_timestamp datetime,
order_estimated_delivery_date datetime
);

if object_id('sales.C_Payments' , 'U') is not null
	drop table sales.C_Payments;
Create table sales.C_Payments(
order_id nvarchar(50),
payment_sequential int,
payment_type nvarchar(50),
payment_installments int,
payment_value float
);

if object_id('sales.C_Payments' , 'U') is not null
	drop table sales.C_Payments;
Create table sales.C_Payments(
order_id nvarchar(50),
payment_sequential int,
payment_type nvarchar(50),
payment_installments int,
payment_value float
);

if object_id('sales.C_Products' , 'U') is not null
	drop table sales.C_Products;
Create table sales.C_Products(
product_id nvarchar(50),
product_category_name nvarchar(50),
product_weight_g float,
product_length_cm float,
product_height_cm float,
product_width_cm float
);

Bulk Insert sales.C_Customers
from 'D:\Bahan Belajar\Project\Retail_dataset\C_Customers.csv'
with (
	format = 'CSV',
    firstrow = 2,
    fieldterminator = ',',
    tablock
)

Bulk Insert sales.C_Order_Items
from 'D:\Bahan Belajar\Project\Retail_dataset\C_Order_Items.csv'
with (
	format = 'CSV',
    firstrow = 2,
    fieldterminator = ',',
    tablock
)

Bulk Insert sales.C_Orders
from 'D:\Bahan Belajar\Project\Retail_dataset\C_Orders.csv'
with (
	format = 'CSV',
    firstrow = 2,
    fieldterminator = ',',
    tablock
)

Bulk Insert sales.C_Payments
from 'D:\Bahan Belajar\Project\Retail_dataset\C_Payments.csv'
with (
	format = 'CSV',
    firstrow = 2,
    fieldterminator = ',',
    tablock
)

Bulk Insert sales.C_Products
from 'D:\Bahan Belajar\Project\Retail_dataset\C_Products.csv'
with (
	format = 'CSV',
    firstrow = 2,
    fieldterminator = ',',
    tablock
)

-------------------------------------------------------------------------------------------------------------------


-- Cek Order ID status delivered, shipped, invoiced yang tidak memiliki detail order item

select 
o.order_id
from sales.C_Orders o
left join sales.Orders_item oi
on o.order_id = oi.order_id
where oi.order_id is null and (order_status = 'Delivered' or order_status = 'Shipped' or order_status = 'Invoiced')


--Cek product ID di tabel Order Items yang tidak ada di tabel Products

select
oi.product_id
from sales.Orders_item oi
left join sales.Products p
on oi.product_id = p.product_id
where p.product_id is null


--Cek Order ID yang tidak memiliki detail Payments

select 
o.order_id
from sales.C_Orders o
left join sales.C_Payments p
on o.order_id = p.order_id
left join sales.C_Order_Items oi
on o.order_id = oi.order_id
where p.order_id is null


--Cek Customer ID di tabel Order yang tidak ada di tabel Customer

select
o.customer_id
from sales.C_Orders o
left join sales.C_Customers c
on o.customer_id = c.customer_id
where c.customer_id is null


--Total Revenue Per Months

with cte as(
    select
    month(o.order_purchase_timestamp) as months,
    year(o.order_purchase_timestamp) as years,
    sum(p.payment_value) as total_sales
    from sales.C_Orders o
    left join sales.C_Payments p
    on o.order_id = p.order_id
    where o.order_status = 'Delivered'
    group by month(o.order_purchase_timestamp), year(o.order_purchase_timestamp))
select
months,
years,
case
    when sum(total_sales) > 1000000 then '$' + format(sum(total_sales) / 1000000.0, '0.##') + 'M'
    when sum(total_sales) > 1000 then '$' + format(sum(total_sales) / 1000.0, '0.##') + 'K'
    else '$' + format(sum(total_sales), '0.##')
end total_sales
from cte
group by months, years
order by sum(total_sales) desc


--Rata2 lama pengiriman

select
    avg(cast(datediff(day, order_purchase_timestamp, order_delivered_timestamp) as decimal(10,2))) 
        as avg_delivery
from sales.C_Orders
where order_status = 'Delivered'


--Total null dari ket waktu

select
order_status,
sum(case when order_purchase_timestamp is null then 1 else 0 end) as order_purchase,
sum(case when order_approved_at is null then 1 else 0 end) as order_approved,
sum(case when order_delivered_timestamp is null then 1 else 0 end) as order_delivered,
sum(case when order_estimated_delivery_date is null then 1 else 0 end) as order_delivered_estimated,
count(*) as count_of_order
from sales.C_Orders
group by order_status


--Melihat perilaku canceled

select
order_status,
sum(case when order_approved_at is null and order_delivered_timestamp is null then 1 else 0 end)
    as stop_before_approved,
sum(case when order_approved_at is not null and order_delivered_timestamp is null then 1 else 0 end) 
    as stop_before_delivered,
sum(case when order_approved_at is not null and order_delivered_timestamp is not null then 1 else 0 end) 
    as stop_after_delivered
from sales.C_Orders
where order_status = 'Canceled'
group by order_status

--Total customer, total order

select
count(*) as total_customer
from sales.C_Customers

select
count(*) as total_order
from sales.C_Orders


--Payment type total

select
payment_type,
count(*) as total_transaction
from sales.C_Payments
group by payment_type


--Penjualan per produk kategori

select
p.product_category_name,
count(oi.order_item_id) as total_sold_item,
case
    when sum(py.payment_value) >= 1000000000 then '$' + format(sum(py.payment_value) / 1000000000.0,
        '0.##') + 'B'
    when sum(py.payment_value) >= 1000000 then '$' + format(sum(py.payment_value) / 1000000.0,
        '0.##') + 'M'
    when sum(py.payment_value) >= 1000 then '$' + format(sum(py.payment_value) / 1000.0, '0.##') + 'M'
    else '$' + format(sum(py.payment_value), '0.##')
end as total_sales
from sales.C_Products p
left join sales.C_Order_Items oi
on p.product_id = oi.product_id
left join sales.C_Payments py
on oi.order_id = py.order_id
group by p.product_category_name
order by count(oi.order_item_id) desc


--Retention per bulan

with first_purchase as (
select
customer_id,
min(format(order_purchase_timestamp, 'yyyy-MM')) as bulan_pertama
from sales.C_Orders
group by customer_id
)
, retentions as (
select
customer_id,
format(order_purchase_timestamp, 'yyyy-MM') as bulan_retention
from sales.C_Orders
)
, retentions_activity as (
select
fp.bulan_pertama,
r.bulan_retention,
count(distinct r.customer_id) as active_users
from first_purchase fp
join retentions r
on fp.customer_id = r.customer_id
group by fp.bulan_pertama, r. bulan_retention
)
, retentions_size as (
select
bulan_pertama,
count(distinct customer_id) as total_users
from first_purchase
group by bulan_pertama
)
, retention1 as(
select
ra.bulan_pertama,
ra.bulan_retention,
ra.active_users,
rs.total_users,
round((ra.active_users * 1.0 / rs.total_users) * 100, 5) AS retention_rate
from retentions_activity ra
join retentions_size rs
on ra.bulan_pertama = rs.bulan_pertama
where ra.bulan_pertama != ra.bulan_retention and round((ra.active_users * 1.0 / rs.total_users) * 100, 5) != 100)
select
cast(round(avg(retention_rate),3) as varchar(20)) + '%' as avg_retention
from retention1;

------------------------------------------------------------------------------------------------------------------