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



-- B1 : CA mensuel 2025 par catégorie
SELECT d.year_month, p.category, ROUND(SUM(f.line_amount), 2) AS ca
FROM workspace.shopflow.fact_sales AS f
JOIN workspace.shopflow.dim_date AS d USING (date_key)
JOIN workspace.shopflow.dim_product AS p USING (product_id)
WHERE d.year = 2025 AND f.order_status != 'cancelled'
GROUP BY d.year_month, p.category
ORDER BY d.year_month, ca DESC;


-- B2 : taux de fraude par pays de carte (paiements autorisés), top 5 en montant
SELECT
  card_country,
  COUNT(*) AS paiements,
  count_if(is_fraud) AS fraudes,
  ROUND(100 * count_if(is_fraud) / COUNT(*), 2) AS taux_nombre_pct,
  ROUND(100 * SUM(IF(is_fraud, amount, 0)) / NULLIF(SUM(amount), 0), 2) AS taux_montant_pct
FROM  workspace.shopflow.fact_payments
WHERE status = 'authorized'
GROUP BY card_country
ORDER BY taux_montant_pct DESC
LIMIT 5;



-- B3 : profil par scénario de fraude
SELECT
  fraud_scenario,
  COUNT(*) AS paiements,
  ROUND(SUM(amount), 2) AS montant,
  ROUND(100 * count_if(has_chargeback) / NULLIF(count_if(status = 'authorized'), 0), 1) AS pct_retrofacturees,
  ROUND(AVG(chargeback_delay_days), 1) AS delai_moyen_jours
FROM  workspace.shopflow.fact_payments
WHERE is_fraud
GROUP BY fraud_scenario;
