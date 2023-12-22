WITH fechas as (
    SELECT
    member_id,
    --h_sponsor_id,
    bit_reference,
    (SELECT MIN(DATE_SUB(created_ts, INTERVAL 6 HOUR)) FROM
    `daf-datalake-prd-trusted.spinpremia_gravty.tbl_activity_bit` b
  WHERE
    a.member_id = b.member_id 
    AND h_bit_type = 'PURCHASE'
    AND DATE(TIMESTAMP_SUB(created_ts, INTERVAL 6 HOUR)) >= DATE('2023-02-01')
    AND DATE(TIMESTAMP_SUB(created_ts, INTERVAL 6 HOUR)) <= DATE_SUB(CURRENT_DATE('-06'), INTERVAL 1 DAY)
    AND status = 'SUCCESS'
    AND rewarded > 0) As min_date,
    created_ts
  --  DATE_ADD(MIN(DATE_SUB(created_ts, INTERVAL 6 HOUR)), INTERVAL 83 DAY) AS max_date
  FROM
    `daf-datalake-prd-trusted.spinpremia_gravty.tbl_activity_bit` a
  WHERE
    h_bit_type = 'PURCHASE'
    AND DATE(TIMESTAMP_SUB(created_ts, INTERVAL 6 HOUR)) >= DATE('2023-02-01')
    AND DATE(TIMESTAMP_SUB(created_ts, INTERVAL 6 HOUR)) <= DATE_SUB(CURRENT_DATE('-06'), INTERVAL 1 DAY)
    AND status = 'SUCCESS'
    AND rewarded > 0
  GROUP BY
    member_id,
    bit_reference,
    created_ts
    --,h_sponsor_id
  )
  ,redeemed AS (
  SELECT
    (SELECT MIN(DATE_SUB(created_ts, INTERVAL 6 HOUR)) FROM `daf-datalake-prd-trusted.spinpremia_gravty.tbl_activity_bit` c
    WHERE a.member_id = c.member_id
    AND h_bit_type ='PAYMENT_WITH_POINTS'
    AND DATE(TIMESTAMP_SUB(a.created_ts, INTERVAL 6 HOUR)) >= DATE('2023-02-01')
    AND DATE(TIMESTAMP_SUB(a.created_ts, INTERVAL 6 HOUR)) <= DATE_SUB(CURRENT_DATE('-06'), INTERVAL 1 DAY)
    AND status = 'SUCCESS'
    AND c.h_sponsor_id <> 5) AS DateRedeem,
    a.created_ts,
    a.member_id,
 --   min_date,
    b.bit_reference
   -- max_date
  FROM
    `daf-datalake-prd-trusted.spinpremia_gravty.tbl_activity_bit` a
  LEFT
  JOIN fechas AS b
  ON a.member_id = b.member_id
 -- AND a.h_sponsor_id = b.h_sponsor_id
   WHERE
    h_bit_type ='PAYMENT_WITH_POINTS'
    AND DATE(TIMESTAMP_SUB(a.created_ts, INTERVAL 6 HOUR)) >= DATE('2023-02-01')
    AND DATE(TIMESTAMP_SUB(a.created_ts, INTERVAL 6 HOUR)) <= DATE_SUB(CURRENT_DATE('-06'), INTERVAL 1 DAY)
    AND status = 'SUCCESS'
    AND a.h_sponsor_id <> 5
    AND DATE_SUB(a.created_ts, INTERVAL 6 HOUR) >= min_date 
    GROUP BY
    a.created_ts,
    member_id,
    b.bit_reference
  --  ,min_date
    ), 
    tsx AS (
      SELECT
      a.member_id,
      a.created_ts,
  --    min_date,
   --   COUNT(DISTINCT a.bit_reference) AS count_bit
      a.bit_reference
      FROM
      fechas as a 
      LEFT JOIN redeemed AS b
      ON a.member_id = b.member_id
      WHERE a.created_ts >= min_date 
      AND a.created_ts < DateRedeem
  --     GROUP BY 
  --  --   a.member_id,
  --     a.created_ts,
  --     min_date,
  --     a.bit_reference
    )
    , kpi AS (
      SELECT
   decile,
   COUNT(DISTINCT member_id) AS members,
   CONCAT(MIN(count_tsx), " - ", MAX(count_tsx)) AS ranges_deciles,
  FROM (
    SELECT
      member_id,
      created_ts,
      NTILE(10) OVER (ORDER BY COUNT(bit_reference)) AS decile,
      COUNT(bit_reference) AS count_tsx,
    FROM
      tsx
      GROUP BY
      member_id,
      created_ts
   --   min_date
  ) subquery
  GROUP BY
    decile
    ORDER BY
    decile
   )
SELECT * FROM kpi
