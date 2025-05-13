
from google.cloud import bigquery
import pandas as pd

proj = 'etsy-bigquery-adhoc-prod'
client = bigquery.Client(project = proj)

def query_to_df(sql:str) -> pd.DataFrame:
    query_job = client.query(sql)
    results = query_job.result()
    return results.to_dataframe()

# Get Top-Line Experiment Summary Details
def get_experiment_summary(experiment_id):
  query = f"""

    DECLARE experiment_name STRING;
    SET experiment_name = '{experiment_id}';

    WITH max_values_cte AS (
      SELECT
        experiment_id,
        boundary_start_ts,
        max(_date) as end_date,
        is_filtered,
        ROW_NUMBER() OVER(PARTITION BY experiment_id ORDER BY boundary_start_ts DESC) AS row_num
      FROM `etsy-data-warehouse-prod.catapult_unified.experiment`
      WHERE
        experiment_id = experiment_name
    --         AND _date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
        group by 1,2,4
    ),

    boundary as (SELECT
      a.experiment_id `Experiment ID`,
      b.name `Name`,
      DATE(a.boundary_start_ts) AS `Start Date`,
      a.end_date `End Date`,
      a.is_filtered `Is Filtered`
    FROM max_values_cte a
    JOIN `etsy-data-warehouse-prod.etsy_atlas.catapult_launches` b ON a.experiment_id = b.config_flag
    WHERE a.row_num = 1
    group by 1,2,3,4,5
    )

    select * from boundary
    """
  
  df = query_to_df(query)
  return df



# Get Stats on Individual Experiment Variants
def get_variant_stats(experiment_id):
  query = f"""
  DECLARE experiment_name STRING;
  SET experiment_name = '{experiment_id}';

  WITH catapult_experiment_receipts AS (WITH max_values_cte AS (
      SELECT
          experiment_id,
          boundary_start_ts,
          max(_date) as end_date,
          is_filtered,
          ROW_NUMBER() OVER(PARTITION BY experiment_id ORDER BY boundary_start_ts DESC) AS row_num
        FROM `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
          experiment_id = experiment_name
  --         AND _date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
          group by 1,2,4
      ),

      boundary as (SELECT
        a.experiment_id,
        b.name,
        a.boundary_start_ts AS boundary_start_ts,
        DATE(a.boundary_start_ts) AS start_date,
        a.end_date,
        a.is_filtered
      FROM max_values_cte a
      JOIN `etsy-data-warehouse-prod.etsy_atlas.catapult_launches` b ON a.experiment_id = b.config_flag
      WHERE a.row_num = 1
      group by 1,2,3,4,5,6
      ),

      bucketing_exp as (select
          e.experiment_id,
          b.name,
          case when e.experiment_id like '%_sf' or b.name like '%[Seller-Facing]%' then 0 else 1 end as buyer_focused,
          e.boundary_start_ts,
          case when b.is_filtered = true then e.filtered_bucketing_ts else e.bucketing_ts end as bucketing_time,
          case when b.is_filtered = true then date(e.filtered_bucketing_ts) else date(e.bucketing_ts) end as bucketing_date,
          b.is_filtered,
          e.bucketing_ts,
          e.filtered_bucketing_ts,
          e.bucketing_id,
          e.variant_id,
          e.bucketing_id_type,
          b.end_date,
          TIMESTAMP_ADD(TIMESTAMP(b.end_date), INTERVAL 1439 MINUTE) as end_time
        from `etsy-data-warehouse-prod.catapult_unified.bucketing_period` e
        join boundary b using (experiment_id, boundary_start_ts)
        WHERE
          experiment_id = experiment_name
  --         AND _date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
      ),

      tab as (
      (select
        a.experiment_id
        , a.name
        , a.bucketing_id
        , a.bucketing_id_type
        , a.variant_id
        , a.bucketing_date
        , date(b.creation_tsz) receipt_date
        , b.receipt_id
      FROM
        bucketing_exp a
        LEFT JOIN (
          SELECT
            vt._date
            , vt.purch_date
            , (split(vt.visit_id, '.')[ORDINAL(1)]) as browser_id
            , vt.user_id
            , vt.seller_user_id
            , vt.receipt_id
            , ar.creation_tsz
          FROM `etsy-data-warehouse-prod.visit_mart.visits_transactions` vt
          LEFT JOIN `etsy-data-warehouse-prod.transaction_mart.all_receipts` ar
          ON vt.receipt_id = ar.receipt_id
  --         WHERE vt._date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
          ) b
        ON a.bucketing_id = b.browser_id
        AND b.creation_tsz BETWEEN a.bucketing_time and a.end_time
  --       AND b._date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
        WHERE (experiment_id = experiment_name)
        AND a.bucketing_id_type = 1
        AND a.bucketing_time is not null)
        UNION ALL
      (select
        a.experiment_id
        , a.name
        , a.bucketing_id
        , a.bucketing_id_type
        , a.variant_id
        , a.bucketing_date
        , date(b.creation_tsz) receipt_date
        , b.receipt_id
      FROM
        bucketing_exp a
        LEFT JOIN (
          SELECT
            vt._date
            , vt.purch_date
            , (split(vt.visit_id, '.')[ORDINAL(1)]) as browser_id
            , vt.user_id
            , vt.seller_user_id
            , vt.receipt_id
            , ar.creation_tsz
          FROM `etsy-data-warehouse-prod.visit_mart.visits_transactions` vt
          LEFT JOIN `etsy-data-warehouse-prod.transaction_mart.all_receipts` ar
          ON vt.receipt_id = ar.receipt_id
  --         WHERE vt._date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
          ) b
        ON CAST(a.bucketing_id AS INT64) = b.user_id
        AND b.creation_tsz BETWEEN a.bucketing_time and a.end_time
  --       AND b._date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
        WHERE (experiment_id = experiment_name)
        AND a.bucketing_id_type = 2
        AND a.buyer_focused = 1
        AND a.bucketing_time is not null)
        UNION ALL
      (select
        a.experiment_id
        , a.name
        , a.bucketing_id
        , a.bucketing_id_type
        , a.variant_id
        , a.bucketing_date
        , date(b.creation_tsz) receipt_date
        , b.receipt_id
      FROM
        bucketing_exp a
        LEFT JOIN (
          SELECT
            vt._date
            , vt.purch_date
            , (split(vt.visit_id, '.')[ORDINAL(1)]) as browser_id
            , vt.user_id
            , vt.seller_user_id
            , vt.receipt_id
            , ar.creation_tsz
          FROM `etsy-data-warehouse-prod.visit_mart.visits_transactions` vt
          LEFT JOIN `etsy-data-warehouse-prod.transaction_mart.all_receipts` ar
          ON vt.receipt_id = ar.receipt_id
  --         WHERE vt._date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
          ) b
        ON CAST(a.bucketing_id AS INT64) = b.seller_user_id
        AND b.creation_tsz BETWEEN a.bucketing_time and a.end_time
  --       AND b._date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
        WHERE (experiment_id = experiment_name)
        AND a.bucketing_id_type = 2
        AND a.buyer_focused = 0
        AND a.bucketing_time is not null)
      )

      select
        experiment_id
        , name
        , bucketing_id
        , bucketing_id_type
        , variant_id
        , bucketing_date
        , receipt_id
        , min(receipt_date) as receipt_date
      from tab group by 1,2,3,4,5,6,7
      )
    ,  all_receipts AS (with receipts_marked_as_gift as (
          select
          receipt_id
          , max(is_gift) as marked_as_gift # 1 if any transaction in a receipt is marked as a gift
          from `etsy-data-warehouse-prod.transaction_mart.all_transactions`
          where receipt_id is not null
          group by 1
        )
        select a.*,
          c.name as buyer_country_name,
          b.gms_gross,
          b.gms_net,
          b.seller_country_name,
          b.is_gift_card,
          d.user_case_id,
          case when d.user_case_id is not null then 1 else 0 end as has_case,
          d.type,
          timestamp_seconds(d.case_date) as case_date,
          case when e.receipt_id is not null then 1 else 0 end as has_gift_teaser,
          marked_as_gift
        from `etsy-data-warehouse-prod`.transaction_mart.all_receipts a
        join `etsy-data-warehouse-prod`.transaction_mart.receipts_gms b  on a.receipt_id = b.receipt_id
        left join `etsy-data-warehouse-prod`.etsy_v2.countries c on a.country_id=c.country_id
        left join `etsy-data-warehouse-prod`.rollups.user_cases d on a.receipt_id = d.receipt_id
        left join `etsy-data-warehouse-prod.etsy_shard.gift_receipt_options` e on a.receipt_id = e.receipt_id
        left join receipts_marked_as_gift f on a.receipt_id = f.receipt_id
      )
  SELECT
      catapult_experiment_receipts.experiment_id `Experiment ID`
      , catapult_experiment_receipts.variant_id  AS `Variant`
      , count(distinct(catapult_experiment_receipts.bucketing_id)) `Bucketed Users`
      , sum(COALESCE(cast(all_receipts.gms_gross as float64), 0)) AS `GMS`
  FROM catapult_experiment_receipts
  LEFT JOIN all_receipts ON catapult_experiment_receipts.receipt_id = all_receipts.receipt_id
  LEFT JOIN (select receipt_id, sum(case when cor_stream in ('bad_recoupment') then amount_usd else null end) brs, sum(case when cor_stream in ('chargeback', 'chargeback_fee') then amount_usd else null end) cbs from `etsy-data-warehouse-prod.rollups.cor_main` group by 1) cor ON catapult_experiment_receipts.receipt_id = cor.receipt_id
  WHERE (catapult_experiment_receipts.experiment_id ) = experiment_name
  -- and catapult_experiment_receipts.receipt_id is not null
  group by 1, 2
  """

  df = query_to_df(query)
  return df



# Get All Experiment Receipts
def get_experiment_receipts(experiment_id, variant1, variant2):
  query = f"""
  DECLARE experiment_name STRING;
  SET experiment_name = '{experiment_id}';

  WITH catapult_experiment_receipts AS (WITH max_values_cte AS (
        SELECT
          experiment_id,
          boundary_start_ts,
          max(_date) as end_date,
          is_filtered,
          ROW_NUMBER() OVER(PARTITION BY experiment_id ORDER BY boundary_start_ts DESC) AS row_num
        FROM `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
          experiment_id = experiment_name
  --         AND _date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
          group by 1,2,4
      ),

      boundary as (SELECT
        a.experiment_id,
        b.name,
        a.boundary_start_ts AS boundary_start_ts,
        DATE(a.boundary_start_ts) AS start_date,
        a.end_date,
        a.is_filtered
      FROM max_values_cte a
      JOIN `etsy-data-warehouse-prod.etsy_atlas.catapult_launches` b ON a.experiment_id = b.config_flag
      WHERE a.row_num = 1
      group by 1,2,3,4,5,6
      ),

      bucketing_exp as (select
          e.experiment_id,
          b.name,
          case when e.experiment_id like '%_sf' or b.name like '%[Seller-Facing]%' then 0 else 1 end as buyer_focused,
          e.boundary_start_ts,
          case when b.is_filtered = true then e.filtered_bucketing_ts else e.bucketing_ts end as bucketing_time,
          case when b.is_filtered = true then date(e.filtered_bucketing_ts) else date(e.bucketing_ts) end as bucketing_date,
          b.is_filtered,
          e.bucketing_ts,
          e.filtered_bucketing_ts,
          e.bucketing_id,
          e.variant_id,
          e.bucketing_id_type,
          b.end_date,
          TIMESTAMP_ADD(TIMESTAMP(b.end_date), INTERVAL 1439 MINUTE) as end_time
        from `etsy-data-warehouse-prod.catapult_unified.bucketing_period` e
        join boundary b using (experiment_id, boundary_start_ts)
        WHERE
          experiment_id = experiment_name
  --         AND _date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
      ),

      tab as (
      (select
        a.experiment_id
        , a.name
        , a.bucketing_id
        , a.bucketing_id_type
        , a.variant_id
        , a.bucketing_date
        , date(b.creation_tsz) receipt_date
        , b.receipt_id
      FROM
        bucketing_exp a
        LEFT JOIN (
          SELECT
            vt._date
            , vt.purch_date
            , (split(vt.visit_id, '.')[ORDINAL(1)]) as browser_id
            , vt.user_id
            , vt.seller_user_id
            , vt.receipt_id
            , ar.creation_tsz
          FROM `etsy-data-warehouse-prod.visit_mart.visits_transactions` vt
          LEFT JOIN `etsy-data-warehouse-prod.transaction_mart.all_receipts` ar
          ON vt.receipt_id = ar.receipt_id
  --         WHERE vt._date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
          ) b
        ON a.bucketing_id = b.browser_id
        AND b.creation_tsz BETWEEN a.bucketing_time and a.end_time
  --       AND b._date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
        WHERE (experiment_id = experiment_name)
        AND a.bucketing_id_type = 1
        AND a.bucketing_time is not null)
        UNION ALL
      (select
        a.experiment_id
        , a.name
        , a.bucketing_id
        , a.bucketing_id_type
        , a.variant_id
        , a.bucketing_date
        , date(b.creation_tsz) receipt_date
        , b.receipt_id
      FROM
        bucketing_exp a
        LEFT JOIN (
          SELECT
            vt._date
            , vt.purch_date
            , (split(vt.visit_id, '.')[ORDINAL(1)]) as browser_id
            , vt.user_id
            , vt.seller_user_id
            , vt.receipt_id
            , ar.creation_tsz
          FROM `etsy-data-warehouse-prod.visit_mart.visits_transactions` vt
          LEFT JOIN `etsy-data-warehouse-prod.transaction_mart.all_receipts` ar
          ON vt.receipt_id = ar.receipt_id
  --         WHERE vt._date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
          ) b
        ON CAST(a.bucketing_id AS INT64) = b.user_id
        AND b.creation_tsz BETWEEN a.bucketing_time and a.end_time
  --       AND b._date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
        WHERE (experiment_id = experiment_name)
        AND a.bucketing_id_type = 2
        AND a.buyer_focused = 1
        AND a.bucketing_time is not null)
        UNION ALL
      (select
        a.experiment_id
        , a.name
        , a.bucketing_id
        , a.bucketing_id_type
        , a.variant_id
        , a.bucketing_date
        , date(b.creation_tsz) receipt_date
        , b.receipt_id
      FROM
        bucketing_exp a
        LEFT JOIN (
          SELECT
            vt._date
            , vt.purch_date
            , (split(vt.visit_id, '.')[ORDINAL(1)]) as browser_id
            , vt.user_id
            , vt.seller_user_id
            , vt.receipt_id
            , ar.creation_tsz
          FROM `etsy-data-warehouse-prod.visit_mart.visits_transactions` vt
          LEFT JOIN `etsy-data-warehouse-prod.transaction_mart.all_receipts` ar
          ON vt.receipt_id = ar.receipt_id
  --         WHERE vt._date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
          ) b
        ON CAST(a.bucketing_id AS INT64) = b.seller_user_id
        AND b.creation_tsz BETWEEN a.bucketing_time and a.end_time
  --       AND b._date BETWEEN DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -180 DAY) AND CURRENT_DATE('UTC')
        WHERE (experiment_id = experiment_name)
        AND a.bucketing_id_type = 2
        AND a.buyer_focused = 0
        AND a.bucketing_time is not null)
      )

      select
        experiment_id
        , name
        , bucketing_id
        , bucketing_id_type
        , variant_id
        , bucketing_date
        , receipt_id
        , min(receipt_date) as receipt_date
      from tab group by 1,2,3,4,5,6,7
      )
    ,  all_receipts AS (with receipts_marked_as_gift as (
          select
          receipt_id
          , max(is_gift) as marked_as_gift # 1 if any transaction in a receipt is marked as a gift
          from `etsy-data-warehouse-prod.transaction_mart.all_transactions`
          where receipt_id is not null
          group by 1
        )
        select a.*,
          c.name as buyer_country_name,
          b.gms_gross,
          b.gms_net,
          b.seller_country_name,
          b.is_gift_card,
          d.user_case_id,
          case when d.user_case_id is not null then 1 else 0 end as has_case,
          d.type,
          timestamp_seconds(d.case_date) as case_date,
          case when e.receipt_id is not null then 1 else 0 end as has_gift_teaser,
          marked_as_gift
        from `etsy-data-warehouse-prod`.transaction_mart.all_receipts a
        join `etsy-data-warehouse-prod`.transaction_mart.receipts_gms b  on a.receipt_id = b.receipt_id
        left join `etsy-data-warehouse-prod`.etsy_v2.countries c on a.country_id=c.country_id
        left join `etsy-data-warehouse-prod`.rollups.user_cases d on a.receipt_id = d.receipt_id
        left join `etsy-data-warehouse-prod.etsy_shard.gift_receipt_options` e on a.receipt_id = e.receipt_id
        left join receipts_marked_as_gift f on a.receipt_id = f.receipt_id
      )
  SELECT
      catapult_experiment_receipts.variant_id  AS variant
      , catapult_experiment_receipts.receipt_id
      , COALESCE(cast(all_receipts.gms_gross as float64), 0) AS gms_gross
      , COALESCE(cast(all_receipts.gms_net as float64), 0) AS gms_net
      , COALESCE(cor.brs, 0) bad_recoupments
      , COALESCE(cor.cbs, 0) chargebacks
      , COALESCE(cor.brs, 0) + COALESCE(cor.cbs, 0) total_cor
  FROM catapult_experiment_receipts
  LEFT JOIN all_receipts ON catapult_experiment_receipts.receipt_id = all_receipts.receipt_id
  LEFT JOIN (select receipt_id, sum(case when cor_stream in ('bad_recoupment') then amount_usd else null end) brs, sum(case when cor_stream in ('chargeback', 'chargeback_fee') then amount_usd else null end) cbs from `etsy-data-warehouse-prod.rollups.cor_main` group by 1) cor ON catapult_experiment_receipts.receipt_id = cor.receipt_id
  WHERE (catapult_experiment_receipts.experiment_id ) = experiment_name
  and catapult_experiment_receipts.receipt_id is not null
  and catapult_experiment_receipts.variant_id in ('{variant1}', '{variant2}')
  """

  df = query_to_df(query)
  return df



# Bootstrapping
def bootstrap_sample(data, variant_col, metric_type, control_id, treatment_id, num_iterations):
  if metric_type == 'total':
    metric_col = 'total_cor'
  elif metric_type in ('bad_recoupments', 'chargebacks'):
    metric_col = metric_type
  else:
    print('Error: wrong metric type inputted')

  diff_means = []
  means_control = []
  means_treatment = []

  for _ in range(num_iterations):
      sample = data.sample(frac=1, replace=True)

      mean_control = sample[sample[variant_col] == control_id][metric_col].mean()
      mean_treatment = sample[sample[variant_col] == treatment_id][metric_col].mean()

      means_control.append(mean_control)
      means_treatment.append(mean_treatment)
      diff_means.append(mean_treatment - mean_control)

  return diff_means, means_control, means_treatment

