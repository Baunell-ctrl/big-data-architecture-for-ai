USE workspace.shopflow;

CREATE OR REPLACE TABLE dim_date AS
SELECT
  d AS date_key,
  year(d) AS year,
  quarter(d) AS quarter,
  month(d) AS month,
  date_format(d, 'yyyy-MM') AS year_month,
  dayofweek(d) AS day_of_week,                         -- 1 = dimanche, 7 = samedi
  dayofweek(d) IN (1, 7) AS is_weekend
FROM (SELECT explode(sequence(DATE'2024-01-01', DATE'2025-12-31', INTERVAL 1 DAY)) AS d);

CREATE OR REPLACE TABLE dim_customer AS
SELECT customer_id, coalesce(country, 'Inconnu') AS country, segment, signup_date,
       customer_id = 0 AS is_guest
FROM customers;

CREATE OR REPLACE TABLE dim_product AS
SELECT product_id, category, brand, price AS list_price,
       CASE WHEN price < 20 THEN '<20' WHEN price < 100 THEN '20-100'
            WHEN price < 500 THEN '100-500' ELSE '>500' END AS price_band
FROM products;

CREATE OR REPLACE TABLE fact_sales AS
SELECT i.order_id, i.line_no, to_date(o.order_ts) AS date_key, o.customer_id, i.product_id,
       i.quantity, i.unit_price, round(i.quantity * i.unit_price, 2) AS line_amount,
       o.status AS order_status
FROM order_items AS i JOIN orders AS o USING (order_id);

CREATE OR REPLACE TABLE fact_payments AS
SELECT payment_id, order_id, to_date(payment_ts) AS date_key, payment_ts, customer_id,
       card_country, payment_method, status, amount, is_fraud, fraud_scenario,
       chargeback_ts IS NOT NULL AS has_chargeback,
       timestampdiff(DAY, payment_ts, chargeback_ts) AS chargeback_delay_days
FROM payments;
