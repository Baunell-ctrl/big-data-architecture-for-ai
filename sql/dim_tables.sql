CREATE OR REPLACE TABLE shopflow.dim_date AS
SELECT
  d AS date_key,
  EXTRACT(YEAR FROM d) AS year,
  EXTRACT(QUARTER FROM d) AS quarter,
  EXTRACT(MONTH FROM d) AS month,
  FORMAT_DATE('%Y-%m', d) AS year_month,
  EXTRACT(DAYOFWEEK FROM d) AS day_of_week,          -- 1 = dimanche, 7 = samedi
  EXTRACT(DAYOFWEEK FROM d) IN (1, 7) AS is_weekend
FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2025-12-31')) AS d;

CREATE OR REPLACE TABLE shopflow.dim_customer AS
SELECT
  customer_id,
  COALESCE(country, 'Inconnu') AS country,
  segment,
  signup_date,
  customer_id = 0 AS is_guest
FROM SRC.customers;

CREATE OR REPLACE TABLE shopflow.dim_product AS
SELECT
  product_id, category, brand,
  price AS list_price,
  CASE WHEN price < 20 THEN '<20'
       WHEN price < 100 THEN '20-100'
       WHEN price < 500 THEN '100-500'
       ELSE '>500' END AS price_band
FROM SRC.products;

CREATE OR REPLACE TABLE shopflow.fact_sales AS
SELECT
  i.order_id, i.line_no,
  DATE(o.order_ts) AS date_key,
  o.customer_id, i.product_id, i.quantity, i.unit_price,
  ROUND(i.quantity * i.unit_price, 2) AS line_amount,
  o.status AS order_status
FROM SRC.order_items AS i
JOIN SRC.orders AS o USING (order_id);

CREATE OR REPLACE TABLE shopflow.fact_payments AS
SELECT
  payment_id, order_id,
  DATE(payment_ts) AS date_key,
  payment_ts, customer_id, card_country, payment_method, status, amount,
  is_fraud, fraud_scenario,
  chargeback_ts IS NOT NULL AS has_chargeback,
  TIMESTAMP_DIFF(chargeback_ts, payment_ts, DAY) AS chargeback_delay_days
FROM SRC.payments;
