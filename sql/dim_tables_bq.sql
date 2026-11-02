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
FROM esigelec-big-data-ia.shopflow.customers;

CREATE OR REPLACE TABLE shopflow.dim_product AS
SELECT
  product_id, category, brand,
  price AS list_price,
  CASE WHEN price < 20 THEN '<20'
       WHEN price < 100 THEN '20-100'
       WHEN price < 500 THEN '100-500'
       ELSE '>500' END AS price_band
FROM esigelec-big-data-ia.shopflow.products;

CREATE OR REPLACE TABLE shopflow.fact_sales AS
SELECT
  i.order_id, i.line_no,
  DATE(o.order_ts) AS date_key,
  o.customer_id, i.product_id, i.quantity, i.unit_price,
  ROUND(i.quantity * i.unit_price, 2) AS line_amount,
  o.status AS order_status
FROM esigelec-big-data-ia.shopflow.order_items AS i
JOIN esigelec-big-data-ia.shopflow.orders AS o USING (order_id);

CREATE OR REPLACE TABLE shopflow.fact_payments AS
SELECT
  payment_id, order_id,
  DATE(payment_ts) AS date_key,
  payment_ts, customer_id, card_country, payment_method, status, amount,
  is_fraud, fraud_scenario,
  chargeback_ts IS NOT NULL AS has_chargeback,
  TIMESTAMP_DIFF(chargeback_ts, payment_ts, DAY) AS chargeback_delay_days
FROM esigelec-big-data-ia.shopflow.payments;





-- B1 : CA mensuel 2025 par catégorie
SELECT d.year_month, p.category, ROUND(SUM(f.line_amount), 2) AS ca
FROM shopflow.fact_sales AS f
JOIN shopflow.dim_date AS d USING (date_key)
JOIN shopflow.dim_product AS p USING (product_id)
WHERE d.year = 2025 AND f.order_status != 'cancelled'
GROUP BY d.year_month, p.category
ORDER BY d.year_month, ca DESC;

-- B2 : taux de fraude par pays de carte (paiements autorisés), top 5 en montant
SELECT
  card_country,
  COUNT(*) AS paiements,
  COUNTIF(is_fraud) AS fraudes,
  ROUND(100 * COUNTIF(is_fraud) / COUNT(*), 2) AS taux_nombre_pct,
  ROUND(100 * SUM(IF(is_fraud, amount, 0)) / SUM(amount), 2) AS taux_montant_pct
FROM shopflow.fact_payments
WHERE status = 'authorized'
GROUP BY card_country
ORDER BY taux_montant_pct DESC
LIMIT 5;

-- B3 : profil par scénario de fraude
SELECT
  fraud_scenario,
  COUNT(*) AS paiements,
  ROUND(SUM(amount), 2) AS montant,
  ROUND(100 * COUNTIF(has_chargeback) / COUNTIF(status = 'authorized'), 1) AS pct_retrofacturees,
  ROUND(AVG(chargeback_delay_days), 1) AS delai_moyen_jours
FROM shopflow.fact_payments
WHERE is_fraud
GROUP BY fraud_scenario;