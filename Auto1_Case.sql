CREATE OR REPLACE TEMP TABLE final_analysis_cohort
AS
WITH Base AS(
  SELECT
    *
    , ROW_NUMBER() OVER (PARTITION BY car_id_claims, claim_number ORDER BY selling_date DESC, voucher_amount DESC) AS dedup_col
  FROM `fernando-personal-learning.dbt_fmarquez.car_claims` cl
  JOIN `fernando-personal-learning.dbt_fmarquez.car_sales`  s
    ON cl.car_id_claims = s.car_id_sales
  JOIN `fernando-personal-learning.dbt_fmarquez.car_details` d
    ON cl.car_id_claims=d.car_id_details  
),
dup_check AS(
  SELECT 
    car_id_claims
    , COUNT(*)
  FROM Base
  WHERE 1=1
    AND voucher_amount IS NOT NULL
    AND claim_status NOT IN ('REJECTED','CLOSED_NO_COMPENSATION','CREATED')
    AND dedup_col = 1
  GROUP BY 1
  HAVING COUNT(*) >1
),
Base_deduped AS(
  SELECT *
  FROM Base
    WHERE 1=1
      AND voucher_amount IS NOT NULL
      AND claim_status NOT IN ('REJECTED','CLOSED_NO_COMPENSATION','CREATED')
      AND dedup_col = 1
), 
base_final AS (
SELECT
    car_id_claims AS car_id
    , claim_number
    , full_refund
    , partial_refund
    , voucher_amount
    , claim_status
    , country
    , merchant_id
    , selling_date
    , selling_week
    , payment_date
    , sell_price
    , transport_to_merchant
    , has_tuning
    , has_airbags
    , has_alarm_system
    , fuel_type
    , gear_type
    , ac_type
    , xenon_light
    , navigation_system
    , radio_system
FROM Base_deduped
),
--Here the diagnosis section starts
Biz_global AS(
SELECT 
  COUNT(DISTINCT car_id) AS total_refunds,
  SUM(voucher_amount) AS total_refund_cost,
  AVG(voucher_amount) AS avg_refund_amt,
  ROUND(SAFE_DIVIDE(SUM(voucher_amount)*100,SUM(sell_price)),2) Refund_to_price_pct
FROM base_final
),
by_country AS(
SELECT 
  country,
  COUNT(DISTINCT car_id) AS total_refunds,
  SUM(voucher_amount) AS total_refund_cost,
  AVG(voucher_amount) AS avg_refund_amt,
  ROUND(SAFE_DIVIDE(SUM(voucher_amount)*100,SUM(sell_price)),2) Refund_to_price_pct
FROM base_final
GROUP BY country
ORDER BY 3 DESC, 2 DESC
),
by_merchant AS(
SELECT 
  country,
  merchant_id,
  COUNT(DISTINCT car_id) AS total_refunds,
  SUM(voucher_amount) AS total_refund_cost,
  AVG(voucher_amount) AS avg_refund_amt,
  ROUND(SAFE_DIVIDE(SUM(voucher_amount)*100,SUM(sell_price)),2) Refund_to_price_pct
FROM base_final
WHERE 1=1
  AND country IN('DE','FR')
GROUP BY country,merchant_id
ORDER BY 1, 4 DESC, 3 DESC
), 
merchant_summ AS(
SELECT
  m.country
  , merchant_id
  , m.total_refunds
  , ROUND(SAFE_DIVIDE(m.total_refunds*100,c.total_refunds),2) AS pct_refund_vol
  , m.total_refund_cost
  , ROUND(SAFE_DIVIDE(m.total_refund_cost*100,c.total_refund_cost),2) AS pct_refund_cost
  , m.avg_refund_amt
  , m.Refund_to_price_pct
FROM by_merchant m
LEFT JOIN by_country c
  ON m.country = c.country
ORDER BY 1, 6 DESC,4 DESC
),
total_num_of_merchants_by_country AS(
SELECT
   COUNT(DISTINCT CASE WHEN country = 'DE' THEN merchant_id ELSE NULL END)
   , COUNT(DISTINCT CASE WHEN country = 'FR' THEN merchant_id ELSE NULL END)
FROM merchant_summ
),
merchants_three_cuarters_of_cost AS(
SELECT
  *
  , SUM(pct_refund_cost) OVER(PARTITION BY country ORDER BY pct_refund_cost DESC) AS cumm_sum
FROM merchant_summ
QUALIFY SUM(pct_refund_cost) OVER(PARTITION BY country ORDER BY pct_refund_cost DESC) <=75
ORDER BY country, cumm_sum DESC
), used_merchants_for_analysis AS(
SELECT
   COUNT(DISTINCT CASE WHEN country = 'DE' THEN merchant_id ELSE NULL END)
   , COUNT(DISTINCT CASE WHEN country = 'FR' THEN merchant_id ELSE NULL END)
FROM merchants_three_cuarters_of_cost
)
SELECT
  *
FROM base_final
WHERE 1=1
  AND country IN ('DE','FR')
  AND merchant_id IN (SELECT DISTINCT merchant_id FROM merchants_three_cuarters_of_cost)
;


-- Boolean feature analysis
with boolean_melted AS (
  SELECT country,
         CASE 
           WHEN full_refund = 1 THEN 'Full Refund'
           WHEN partial_refund = 1 THEN 'Partial Refund'
           ELSE 'Unknown'
         END AS return_type,
         'has_tuning' AS feature,
         has_tuning AS value
  FROM final_analysis_cohort
  UNION ALL
  SELECT country, 
         CASE 
           WHEN full_refund = 1 THEN 'Full Refund'
           WHEN partial_refund = 1 THEN 'Partial Refund'
           ELSE 'Unknown'
         END, 'has_airbags', has_airbags
  FROM final_analysis_cohort
  UNION ALL
  SELECT country, 
         CASE 
           WHEN full_refund = 1 THEN 'Full Refund'
           WHEN partial_refund = 1 THEN 'Partial Refund'
           ELSE 'Unknown'
         END, 'has_alarm_system', has_alarm_system
  FROM `final_analysis_cohort`
  UNION ALL
  SELECT country, 
         CASE 
           WHEN full_refund = 1 THEN 'Full Refund'
           WHEN partial_refund = 1 THEN 'Partial Refund'
           ELSE 'Unknown'
         END, 'xenon_light', xenon_light
  FROM final_analysis_cohort
),
boolean_summary AS (
  SELECT
    country,
    return_type,
    feature,
    COUNTIF(value = 1) AS count_with_feature,
    COUNT(*) AS total,
    ROUND(COUNTIF(value = 1)*100.0 / COUNT(*), 2) AS pct_with_feature
  FROM boolean_melted
  WHERE return_type != 'Unknown'
  GROUP BY country, return_type, feature
),

-- Categorical feature analysis
categorical_features AS (
  SELECT
    country,
    CASE 
      WHEN full_refund = 1 THEN 'Full Refund'
      WHEN partial_refund = 1 THEN 'Partial Refund'
      ELSE 'Unknown'
    END AS return_type,
    'fuel_type' AS feature,
    CAST(fuel_type AS STRING) AS value
  FROM final_analysis_cohort

  UNION ALL

  SELECT country,
    CASE 
      WHEN full_refund = 1 THEN 'Full Refund'
      WHEN partial_refund = 1 THEN 'Partial Refund'
      ELSE 'Unknown'
    END, 'gear_type', CAST(gear_type AS STRING)
  FROM final_analysis_cohort

  UNION ALL

  SELECT country,
    CASE 
      WHEN full_refund = 1 THEN 'Full Refund'
      WHEN partial_refund = 1 THEN 'Partial Refund'
      ELSE 'Unknown'
    END, 'ac_type', CAST(ac_type AS STRING)
  FROM final_analysis_cohort

  UNION ALL

  SELECT country,
    CASE 
      WHEN full_refund = 1 THEN 'Full Refund'
      WHEN partial_refund = 1 THEN 'Partial Refund'
      ELSE 'Unknown'
    END, 'radio_system', CAST(radio_system AS STRING)
  FROM final_analysis_cohort

  UNION ALL

  SELECT country,
    CASE 
      WHEN full_refund = 1 THEN 'Full Refund'
      WHEN partial_refund = 1 THEN 'Partial Refund'
      ELSE 'Unknown'
    END, 'navigation_system', CAST(navigation_system AS STRING)
  FROM final_analysis_cohort
),
categorical_summary AS (
  SELECT
    country,
    return_type,
    feature,
    value,
    COUNT(*) AS count_value,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY country, return_type, feature), 2) AS pct_with_value
  FROM categorical_features
  WHERE return_type != 'Unknown'
  GROUP BY country, return_type, feature, value
)

-- Final result union
SELECT * FROM boolean_summary
UNION ALL
SELECT 
  country,
  return_type,
  CONCAT(feature, '=', value) AS feature,
  count_value,
  NULL AS total,
  pct_with_value
FROM categorical_summary
ORDER BY country, count_with_feature DESC;
