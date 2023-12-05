WITH
  stores AS (
  SELECT
    CONCAT(DxMega_Square_Code,DxStore_Code) Location_ID,
    KnState,
    KnCity
  FROM
    `${cat_store}` ),
  members AS (
  SELECT
    member_id,
    gender,
    DATE_DIFF(CURRENT_DATE('-06'), date_of_birth, YEAR) + (CASE
        WHEN DATE_DIFF(CURRENT_DATE('-06'), DATE_ADD(date_of_birth, INTERVAL DATE_DIFF(CURRENT_DATE('-06'), date_of_birth, YEAR) YEAR), DAY) >= 0 THEN 0
      ELSE
      -1
    END
      ) AS age
  FROM
    `${member}` AS a
  WHERE
    a._updated_ts = (
    SELECT
      MAX(_updated_ts)
    FROM
      `${member}` mma
    WHERE
      a.member_id = mma.member_id) ),
  dates AS (
  SELECT
    dia_periodo_fin
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2023-02-01', CURRENT_DATE())) AS dia_periodo_fin
  WHERE
    dia_periodo_fin < CURRENT_DATE('-06')
    AND dia_periodo_fin >= '2023-02-01' ),
  ab AS (
  SELECT
    EXTRACT(date
    FROM
      TIMESTAMP_ADD(created_ts, INTERVAL -6 HOUR)) AS dia_date,
    created_ts,
    CASE
      WHEN h_location = '164OG' THEN REGEXP_EXTRACT(h_bit_source_generated_id, r'^([^_]+)')
      WHEN h_sponsor_id = 6
    AND STARTS_WITH(h_location, '5') THEN LEFT(h_bit_source_generated_id, 10)
    ELSE
    h_location
  END
    AS h_location,
    h_bit_type,
    CASE
      WHEN h_sponsor_id = 10 THEN 4
      WHEN h_location = '164OG' THEN 7
    ELSE
    h_sponsor_id
  END
    AS h_sponsor_id,
    bit_reference,
    member_id
  FROM
    `${activity_bit}`
  WHERE
    status = 'SUCCESS'
    AND h_bit_type IN ('PURCHASE',
      'PAYMENT_WITH_POINTS',
      'PUNCHCARD_REDEMPTION')
    AND EXTRACT(date
    FROM
      TIMESTAMP_ADD(created_ts, INTERVAL -6 HOUR)) < CURRENT_DATE('-06')),
  ab_unique_date AS (
  SELECT
    member_id,
    h_sponsor_id,
    dia_date,
    MAX(h_location) AS h_location
  FROM
    ab
  GROUP BY
    1,
    2,
    3),
  users_base AS (
  SELECT
    *,
    DATE_DIFF(dia_periodo_fin, last_tx, DAY) AS days_last,
    DATE_DIFF(dia_periodo_fin, penultimate_tx, DAY) AS days_penultimate,
    CASE
      WHEN DATE_DIFF(dia_periodo_fin, last_tx, DAY) >= 42 THEN 'churn'
      WHEN DATE_DIFF(dia_periodo_fin, last_tx, DAY) < 42
    AND DATE_DIFF(dia_periodo_fin, penultimate_tx, DAY) < 2 * 42 THEN 'activo'
      WHEN DATE_DIFF(dia_periodo_fin, last_tx, DAY) < 42 AND DATE_DIFF(dia_periodo_fin, penultimate_tx, DAY) >= 2 * 42 THEN 'winback'
      WHEN DATE_DIFF(dia_periodo_fin, last_tx, DAY) < 42
    AND DATE_DIFF(dia_periodo_fin, penultimate_tx, DAY) IS NULL THEN 'nuevo_en_aliado'
    ELSE
    'no_identificado'
  END
    AS flag
  FROM (
    SELECT
      member_id,
      dia_periodo_fin,
      h_sponsor_id,
      MAX(h_location) AS h_location,
      MAX(dia_date) AS last_tx,
      MAX(
        CASE
          WHEN DATE_DIFF(dia_periodo_fin, dia_date, DAY) >= 42 THEN dia_date
        ELSE
        NULL
      END
        ) AS penultimate_tx
    FROM (
      SELECT
        member_id,
        dia_periodo_fin,
        dia_date,
        h_sponsor_id,
      IF
        (dia_date = max_date_id, h_location, NULL) AS h_location,
        max_date_id
      FROM (
        SELECT
          member_id,
          dia_periodo_fin,
          dia_date,
          h_sponsor_id,
          h_location,
          MAX(dia_date) OVER (PARTITION BY member_id, dia_periodo_fin, h_sponsor_id) AS max_date_id
        FROM
          ab_unique_date,
          dates
        WHERE
          dia_periodo_fin >= dia_date
          ))
    GROUP BY
      member_id,
      dia_periodo_fin,
      h_sponsor_id
    HAVING
      DATE_DIFF(dia_periodo_fin, last_tx, DAY) < 2 * 42
      AND (DATE_DIFF(dia_periodo_fin, penultimate_tx, DAY) < 2 * 42 or penultimate_tx is null)
      )),
  summary AS (
  SELECT
    CAST(FORMAT_DATE('%Y%m%d', DATE(dia_periodo_fin)) AS INT64) AS KnDate,
    h_sponsor_id AS KnSponsor,
    h_location,
    flag,
    CASE
      WHEN c.gender = 'M' THEN 1
      WHEN c.gender = 'F' THEN 2
      WHEN c.gender = 'Prefiero no decir' THEN 3
      WHEN c.gender IS NULL THEN -1
    ELSE
    0
  END
    AS KnGender,
    CASE
      WHEN age < 18 THEN 1
      WHEN age >= 18
    AND age <= 25 THEN 2
      WHEN age >= 26 AND age <= 35 THEN 3
      WHEN age >= 36
    AND age <= 50 THEN 4
      WHEN age > 50 THEN 5
    ELSE
    -1
  END
    AS KnAge,
    COUNT(DISTINCT a.member_id) AS users
  FROM
    users_base AS a
  LEFT JOIN
    members AS c
  ON
    a.member_id = c.member_id
  GROUP BY
    KnDate,
    KnSponsor,
    h_location,
    flag,
    KnGender,
    KnAge ),
  kpi AS(
  SELECT
    KnDate,
    KnSponsor,
    KnGender,
    IFNULL(s.KnState, 0) AS KnState,
    IFNULL(s.KnCity, 0) AS KnCity,
    KnAge,
    SUM(
    IF
      (flag ='activo', users, 0)) AS MfActive_Users,
    SUM(
    IF
      (flag ='churn', users, 0)) AS MfChurn_Users,
    SUM(
    IF
      (flag ='nuevo_en_aliado', users, 0)) AS MfNew_Users
  FROM
    summary AS a
  LEFT JOIN
    stores AS s
  ON
    a.h_location = s.Location_ID
  GROUP BY
    KnDate,
    KnSponsor,
    KnState,
    KnCity,
    KnGender,
    KnAge
  ORDER BY
    KnDate DESC,
    KnSponsor DESC ),
  avgs AS (
  SELECT
    KnDate,
    KnSponsor,
    KnState,
    KnCity,
    KnGender,
    KnAge,
    AVG(MfActive_Users) OVER (PARTITION BY KnSponsor, KnState, KnCity, KnGender, KnAge ORDER BY KnDate RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS MfActive_Users1,
    AVG(MfChurn_Users) OVER (PARTITION BY KnSponsor, KnState, KnCity, KnGender, KnAge ORDER BY KnDate RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS MfChurn_Users1,
    AVG(MfNew_Users) OVER (PARTITION BY KnSponsor, KnState, KnCity, KnGender, KnAge ORDER BY KnDate RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS MfNew_Users1
  FROM
    kpi )
SELECT
  KnDate,
  KnSponsor,
  KnState,
  KnCity,
  KnGender,
  KnAge,
  SUM(MfActive_Users1) AS MfActive_Users,
  SUM(MfChurn_Users1) AS MfChurn_Users,
  SUM(MfNew_Users1) AS MfNew_Users
FROM
  avgs
GROUP BY
    KnDate,
    KnSponsor,
    KnState,
    KnCity,
    KnGender,
    KnAge
ORDER BY
  1 DESC,
  2 ASC
