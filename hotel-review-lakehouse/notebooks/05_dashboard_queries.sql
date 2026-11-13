-- Queries for the Databricks SQL dashboard

-- 1. Hotels with the most negative mentions (top 10)
SELECT h.hotel_name, h.country, count(*) AS negative_mentions
FROM workspace.hotel_lakehouse.gold_fact_review_aspect fa
JOIN workspace.hotel_lakehouse.gold_dim_hotel h ON fa.hotel_key = h.hotel_key
WHERE fa.polarity = 'negative'
GROUP BY h.hotel_name, h.country
ORDER BY negative_mentions DESC
LIMIT 10;

-- 2. Which aspects guests complain about / praise most
SELECT a.aspect,
       sum(CASE WHEN fa.polarity = 'negative' THEN 1 ELSE 0 END) AS negative,
       sum(CASE WHEN fa.polarity = 'positive' THEN 1 ELSE 0 END) AS positive
FROM workspace.hotel_lakehouse.gold_fact_review_aspect fa
JOIN workspace.hotel_lakehouse.gold_dim_aspect a ON fa.aspect_key = a.aspect_key
GROUP BY a.aspect
ORDER BY negative DESC;

-- 3. Average reviewer score by month
SELECT d.year, d.month, round(avg(f.reviewer_score), 2) AS avg_score, count(*) AS reviews
FROM workspace.hotel_lakehouse.gold_fact_review f
JOIN workspace.hotel_lakehouse.gold_dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 4. Average score by country
SELECT h.country, round(avg(f.reviewer_score), 2) AS avg_score, count(*) AS reviews
FROM workspace.hotel_lakehouse.gold_fact_review f
JOIN workspace.hotel_lakehouse.gold_dim_hotel h ON f.hotel_key = h.hotel_key
GROUP BY h.country
ORDER BY avg_score DESC;

-- 5. How often the AI model agrees with the reviewer's score
SELECT score_label, model_label, count(*) AS n
FROM workspace.hotel_lakehouse.gold_fact_review
WHERE model_label IS NOT NULL
GROUP BY score_label, model_label
ORDER BY score_label, model_label;
