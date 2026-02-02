create schema if not exists olist;

create table if not exists olist.orders_raw(
	 order_id text,
	 customer_id text,
	 order_status text,
	 order_purchase_timestamp text,
	 order_approved_at text,
	 order_delivered_carrier_date text,
	 order_delivered_customer_date text,
	 order_estimated_delivery_date text
);

select * from olist.orders_raw t limit 10;

create table if not exists olist.order_items_raw(
	order_id text,
	order_item_id text,
	product_id text,
	seller_id text,
	shipping_limit_date text,
	price text,
	freight_value text
);

create table if not exists olist.products_raw(
	product_id text,
	product_category_name text,
	product_name_length text,
	product_description_length text,
	product_photos_qty text,
	product_weight_g text,
	product_length_cm text,
	product_height_cm text,
	product_width_cm text
);

create table if not exists olist.category_translation_raw(
	product_category_name text,
	product_category_name_english text
);

create table if not exists olist.customers_raw(
	customer_id text,
	customer_unique_id text,
	customer_zip_code_prefix text,
	customer_city text,
	customer_state text
);

create table if not exists olist.sellers_raw(
	seller_id text,
	seller_zip_code_prefix text,
	seller_city text,
	seller_state text
);

SELECT 'orders_raw' AS table_name, COUNT(*) AS rows FROM olist.orders_raw
UNION ALL SELECT 'order_items_raw', COUNT(*) FROM olist.order_items_raw
UNION ALL SELECT 'products_raw', COUNT(*) FROM olist.products_raw
UNION ALL SELECT 'category_translation_raw', COUNT(*) FROM olist.category_translation_raw
UNION ALL SELECT 'customers_raw', COUNT(*) FROM olist.customers_raw
UNION ALL SELECT 'sellers_raw', COUNT(*) FROM olist.sellers_raw;

CREATE OR REPLACE VIEW olist.fact_sales AS
WITH orders_typed AS (
  SELECT
    order_id,
    customer_id,
    order_status,
    NULLIF(order_purchase_timestamp, '')::timestamp AS purchase_ts,
    NULLIF(order_delivered_customer_date, '')::timestamp AS delivered_customer_ts,
    NULLIF(order_estimated_delivery_date, '')::timestamp AS estimated_delivery_ts
  FROM olist.orders_raw
),
items_typed AS (
  SELECT
    order_id,
    NULLIF(order_item_id,'')::int AS order_item_id,
    product_id,
    seller_id,
    NULLIF(price,'')::numeric(18,2) AS price,
    NULLIF(freight_value,'')::numeric(18,2) AS freight_value
  FROM olist.order_items_raw
),
product_dim AS (
  SELECT
    p.product_id,
    p.product_category_name,
    COALESCE(t.product_category_name_english, 'unknown') AS category_en
  FROM olist.products_raw p
  LEFT JOIN olist.category_translation_raw t
    ON t.product_category_name = p.product_category_name
),
seller_dim AS (
  SELECT
    seller_id,
    seller_city,
    seller_state
  FROM olist.sellers_raw
)

SELECT
  i.order_id,
  i.order_item_id,
  o.customer_id,
  i.seller_id,
  sd.seller_city,
  sd.seller_state,
  i.product_id,
  pd.category_en,

  o.order_status,
  o.purchase_ts::date AS purchase_date,
  o.purchase_ts,
  i.price,
  i.freight_value,

  CASE
    WHEN o.delivered_customer_ts IS NULL OR o.purchase_ts IS NULL THEN NULL
    ELSE EXTRACT(epoch FROM (o.delivered_customer_ts - o.purchase_ts))/86400.0
  END AS delivery_days,

  CASE
    WHEN o.estimated_delivery_ts IS NULL OR o.delivered_customer_ts IS NULL THEN NULL
    ELSE (o.delivered_customer_ts::date - o.estimated_delivery_ts::date)
  END AS delivery_delay_days
FROM items_typed i
JOIN orders_typed o
  ON o.order_id = i.order_id
LEFT JOIN product_dim pd
  ON pd.product_id = i.product_id
LEFT JOIN seller_dim sd
  ON sd.seller_id = i.seller_id;

SELECT COUNT(*) FROM olist.fact_sales;
SELECT * FROM olist.fact_sales LIMIT 10;

drop view olist.fact_sales;
