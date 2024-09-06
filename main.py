from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_caching import Cache
from httpx import Timeout
import snowflake.connector
from snowflake.connector import DictCursor
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import httpx
import asyncio

REDIS_LINK = os.environ['REDIS']
SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
SNOWFLAKE_PASS = os.environ['SNOWFLAKE_PASS']
SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']

config = {
  "CACHE_TYPE": "redis",
  "CACHE_DEFAULT_TIMEOUT": 14400,
  "CACHE_REDIS_URL": REDIS_LINK
}

app = Flask(__name__)
app.config.from_mapping(config)
cache = Cache(app)
CORS(app)


def make_cache_key(*args, **kwargs):
  path = request.path
  args = str(hash(frozenset(request.args.items())))
  return (path + args).encode('utf-8')


def execute_sql(sql_string, **kwargs):
  conn = snowflake.connector.connect(user=SNOWFLAKE_USER,
                                     password=SNOWFLAKE_PASS,
                                     account=SNOWFLAKE_ACCOUNT,
                                     warehouse=SNOWFLAKE_WAREHOUSE,
                                     database="ARBIGRANTS",
                                     schema="DBT")

  sql = sql_string.format(**kwargs)
  res = conn.cursor(DictCursor).execute(sql)
  results = res.fetchall()
  conn.close()
  return results


@app.route('/overview')
@cache.memoize(make_name=make_cache_key)
def overview():
  timeframe = request.args.get('timeframe', 'month')
  timescale = request.args.get('timescale', '6')
  timescale = int(timescale)
  chain = request.args.get('chain', 'all')

  excludes = request.args.getlist('excludes')
  exclude_list = ",".join(f"'{item}'" for item in excludes) if excludes else ""

  current_date = datetime.now()
  previous_month = current_date.replace(day=1) - relativedelta(
    months=timescale)
  start_month = previous_month.strftime('%Y-%m-%d')

  if exclude_list == "":
    cards_query = execute_sql('''
    SELECT {time}_ACTIVE_WALLETS AS ACTIVE_WALLETS,
    PCT_{time}_ACTIVE_WALLETS AS PCT_WALLETS,
    TVL_GRANTEES,
    PCT_TVL,
    {time}_GAS_SPEND AS GAS_SPEND,
    PCT_{time}_GAS_SPEND AS PCT_GAS_SPEND
    FROM ARBIGRANTS.DBT.ARBIGRANTS_{chain}_SUMMARY
   ''',
                              time=timeframe,
                              chain=chain)

    wallets_stat = [{"ACTIVE_WALLETS": cards_query[0]["ACTIVE_WALLETS"]}]

    wallets_pct_stat = [{"PCT_WALLETS": cards_query[0]["PCT_WALLETS"]}]

    tvl_stat = [{"TVL_GRANTEES": cards_query[0]["TVL_GRANTEES"]}]

    tvl_pct_stat = [{"PCT_TVL": cards_query[0]["PCT_TVL"]}]

    gas_stat = [{"GAS_SPEND": cards_query[0]["GAS_SPEND"]}]

    gas_pct_stat = [{"PCT_GAS_SPEND": cards_query[0]["PCT_GAS_SPEND"]}]

    tvl_chart = execute_sql('''
    SELECT DATE, 'total' AS CATEGORY, TVL FROM ARBIGRANTS.DBT.ARBIGRANTS_ALL_{time}_TVL_ARBITRUM_ONE
    WHERE DATE >= '{start_month}'
    UNION ALL
    SELECT DATE, 'grantees' AS CATEGORY, TVL FROM ARBIGRANTS.DBT.ARBIGRANTS_{chain}_{time}_TVL
    WHERE DATE >= '{start_month}'
    ORDER BY DATE
    ''',
                            time=timeframe,
                            start_month=start_month,
                            chain=chain)

    tvl_chart_eth = execute_sql('''
    SELECT DATE, 'total' AS CATEGORY, TVL_ETH FROM ARBIGRANTS.DBT.ARBIGRANTS_ALL_{time}_TVL_ARBITRUM_ONE
    WHERE DATE >= '{start_month}'
    UNION ALL
    SELECT DATE, 'grantees' AS CATEGORY, TVL_ETH FROM ARBIGRANTS.DBT.ARBIGRANTS_{chain}_{time}_TVL
    WHERE DATE >= '{start_month}'
    ORDER BY DATE
    ''',
                                time=timeframe,
                                start_month=start_month,
                                chain=chain)

    tvl_chart_post_grant = execute_sql('''
    SELECT DATE, TVL 
    FROM ARBIGRANTS.DBT.ARBIGRANTS_{chain}_{time}_TVL_POST_GRANT
    WHERE DATE >= '{start_month}'
    ORDER BY DATE
    ''',
                                       time=timeframe,
                                       start_month=start_month,
                                       chain=chain)

    tvl_chart_eth_post_grant = execute_sql('''
    SELECT DATE, TVL_ETH
    FROM ARBIGRANTS.DBT.ARBIGRANTS_{chain}_{time}_TVL_POST_GRANT
    WHERE DATE >= '{start_month}'
    ORDER BY DATE
    ''',
                                           time=timeframe,
                                           start_month=start_month,
                                           chain=chain)

    accounts_chart = execute_sql('''
    SELECT DATE, 'total' AS CATEGORY, ACTIVE_WALLETS FROM ARBIGRANTS.DBT.ARBIGRANTS_ALL_{time}_ACTIVE_WALLETS_ARBITRUM_ONE
    WHERE DATE >= '{start_month}'
    UNION ALL
    SELECT DATE, 'grantees' AS CATEGORY, ACTIVE_WALLETS FROM ARBIGRANTS.DBT.ARBIGRANTS_{chain}_{time}_ACTIVE_WALLETS
    WHERE DATE >= '{start_month}'
    ORDER BY DATE 
    ''',
                                 time=timeframe,
                                 start_month=start_month,
                                 chain=chain)

    accounts_chart_post_grant = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_{chain}_{time}_ACTIVE_WALLETS_POST_GRANT
    WHERE DATE >= '{start_month}'
    ORDER BY DATE 
    ''',
                                            time=timeframe,
                                            start_month=start_month,
                                            chain=chain)

    tvl_pie = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_{chain}_TVL_PIE
    ''',
                          chain=chain)

    accounts_pie = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_{chain}_{time}_WALLETS_PIE
    ''',
                               time=timeframe,
                               chain=chain)

    leaderboard = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_{chain}_{time}_LEADERBOARD
    UNION ALL
    SELECT
        'TOTAL' as project,
        'Total' as category,
        'total' as slug,
        'https://aefsitlkirjpwxayubwd.supabase.co/storage/v1/object/public/Arbigrants%20logos/AF_logomark.png?t=2024-07-03T10%3A53%3A13.645Z' as logo,
        '' as chain,
        SUM(ETH_FEES) as ETH_FEES,
        0 as ETH_FEES_GROWTH,
        SUM(TRANSACTIONS) as TRANSACTIONS,
        0 as TRANSACTIONS_GROWTH,
        SUM(WALLETS) as WALLETS,
        0 as WALLETS_GROWTH,
        SUM(tvl) as tvl,
        SUM(volume) as volume
    FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_LEADERBOARD
    ORDER BY WALLETS DESC
    ''',
                              time=timeframe,
                              chain=chain)

    milestones = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ALL_MILESTONE_SUMMARY
    ''')

    name_list = execute_sql('''
    SELECT NAME FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA
    ''')

    current_time = datetime.now().strftime('%d/%m/%y %H:%M')

    response_data = {
      "time": current_time,
      "wallets_stat": wallets_stat,
      "wallets_pct_stat": wallets_pct_stat,
      "tvl_stat": tvl_stat,
      "tvl_pct_stat": tvl_pct_stat,
      "gas_stat": gas_stat,
      "gas_pct_stat": gas_pct_stat,
      "tvl_chart": tvl_chart,
      "tvl_chart_eth": tvl_chart_eth,
      "accounts_chart": accounts_chart,
      "tvl_chart_post_grant": tvl_chart_post_grant,
      "tvl_chart_eth_post_grant": tvl_chart_eth_post_grant,
      "accounts_chart_post_grant": accounts_chart_post_grant,
      "tvl_pie": tvl_pie,
      "accounts_pie": accounts_pie,
      "leaderboard": leaderboard,
      "milestones": milestones,
      "name_list": name_list,
    }

    return jsonify(response_data)

  else:

    if timeframe == 'week':
      time_param = '7 day'
    elif timeframe == 'month':
      time_param = '1 month'
    else:
      time_param = '1 day'

    cards_query = execute_sql('''
    WITH stats_gen AS (
    WITH all_txns AS (
    SELECT 
    COUNT(DISTINCT FROM_ADDRESS) as all_active_wallets,
    SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS all_gas_spend
    FROM ARBITRUM.RAW.TRANSACTIONS t   
    WHERE BLOCK_TIMESTAMP < CURRENT_DATE
    AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '{time_param}'
    ),
    
    grantee_txns AS (
    SELECT 
    COUNT(DISTINCT FROM_ADDRESS) as grantee_active_wallets,
    SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS grantee_gas_spend
    FROM ARBITRUM.RAW.TRANSACTIONS t
    INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
    ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
    AND BLOCK_TIMESTAMP < CURRENT_DATE
    AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '{time_param}'
    AND c.NAME NOT IN ({exclude_list})
    INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
    ON m.NAME = c.NAME 
    AND m.chain = 'Arbitrum One'
    )
    
    SELECT 
    grantee_active_wallets AS active_wallets,
    grantee_active_wallets/all_active_wallets AS pct_wallets,
    grantee_gas_spend as gas_spend,
    grantee_gas_spend/all_gas_spend as pct_gas_spend
    FROM all_txns, grantee_txns
    ),

    stats_tvl AS (
    WITH all_tvl AS (
    SELECT 
    TVL AS tvl_all
    FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_TOTAL_TVL
    WHERE DATE = current_date
    ),
    
    grantee_tvl AS (
    SELECT 
    SUM(h.TOTAL_LIQUIDITY_USD) AS tvl_grantees
    FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
    INNER JOIN DEFILLAMA.TVL.HISTORICAL_TVL_PER_CHAIN h
    ON h.CHAIN = 'Arbitrum'
    AND date_trunc('day',h.NEAREST_DATE) = current_date
    AND LLAMA_NAME != ''
    AND h.PROTOCOL_NAME LIKE LLAMA_NAME || '%'
    AND m.NAME NOT IN ({exclude_list})
    AND m.CHAIN = 'Arbitrum One'
    )
    
    SELECT 
    tvl_grantees,
    tvl_grantees/tvl_all as pct_tvl
    FROM all_tvl, grantee_tvl
    )

    SELECT * FROM stats_gen, stats_tvl
    ''',
                              time_param=time_param,
                              exclude_list=exclude_list)

    wallets_stat = [{"ACTIVE_WALLETS": cards_query[0]["ACTIVE_WALLETS"]}]

    wallets_pct_stat = [{"PCT_WALLETS": cards_query[0]["PCT_WALLETS"]}]

    tvl_stat = [{"TVL_GRANTEES": cards_query[0]["TVL_GRANTEES"]}]

    tvl_pct_stat = [{"PCT_TVL": cards_query[0]["PCT_TVL"]}]

    gas_stat = [{"GAS_SPEND": cards_query[0]["GAS_SPEND"]}]

    gas_pct_stat = [{"PCT_GAS_SPEND": cards_query[0]["PCT_GAS_SPEND"]}]

    tvl_query = execute_sql('''
    with total AS (
    SELECT 
    DATE,
    'total' as category,
    TVL,
    TVL_ETH
    FROM ARBIGRANTS.DBT.ARBIGRANTS_ALL_{time}_TVL_ARBITRUM_ONE
    WHERE DATE < DATE_TRUNC('day',CURRENT_DATE())
    AND DATE >= to_timestamp('{start_month}', 'yyyy-MM-dd')
    )

    , grantees AS (
    SELECT 
    DATE,
    'grantees' as category,
    SUM(TVL) AS TVL,
    SUM(TVL_ETH) AS TVL_ETH
    FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_TVL_BY_PROJECT
    WHERE NAME NOT IN ({exclude_list})
    AND DATE < DATE_TRUNC('day',CURRENT_DATE())
    AND DATE >= to_timestamp('{start_month}', 'yyyy-MM-dd')
    GROUP BY 1,2
    )

    SELECT * FROM total
    UNION ALL 
    SELECT * FROM grantees
    ORDER BY DATE
    ''',
                            time=timeframe,
                            start_month=start_month,
                            exclude_list=exclude_list)

    tvl_chart = [{
      k: v
      for k, v in item.items() if k != 'TVL_ETH'
    } for item in tvl_query]
    tvl_chart_eth = [{
      k: v
      for k, v in item.items() if k != 'TVL'
    } for item in tvl_query]

    accounts_chart = execute_sql('''
    with total AS (
    SELECT 
    DATE,
    'total' as category,
    ACTIVE_WALLETS
    FROM ARBIGRANTS.DBT.ARBIGRANTS_ALL_{time}_ACTIVE_WALLETS_ARBITRUM_ONE
    WHERE DATE < DATE_TRUNC('{time}',CURRENT_DATE())
    AND DATE >= to_timestamp('{start_month}', 'yyyy-MM-dd')
    )

    , grantees AS (
    SELECT 
    DATE,
    'grantees' as category,
    SUM(ACTIVE_WALLETS) AS ACTIVE_WALLETS
    FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_ACTIVE_WALLETS_BY_PROJECT
    WHERE DATE < DATE_TRUNC('{time}',CURRENT_DATE())
    AND DATE >= to_timestamp('{start_month}', 'yyyy-MM-dd')
    AND NAME NOT IN ({exclude_list})
    GROUP BY 1,2
    )

    SELECT * FROM total
    UNION ALL 
    SELECT * FROM grantees
    ORDER BY DATE
    ''',
                                 time=timeframe,
                                 start_month=start_month,
                                 exclude_list=exclude_list)

    tvl_post_grant_query = execute_sql('''
    with grantees AS (
    SELECT 
    TO_VARCHAR(DATE_TRUNC('{time}',DATE), 'YYYY-MM-DD') AS date,
    SUM(TVL) AS TVL
    FROM 
    (
        SELECT
        DATE,
        m.GRANT_DATE,
        h.TOTAL_LIQUIDITY_USD AS TVL,
        ROW_NUMBER() OVER (PARTITION BY h.PROTOCOL_NAME, DATE ORDER BY h.NEAREST_DATE DESC) AS rn
        FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
        INNER JOIN DEFILLAMA.TVL.HISTORICAL_TVL_PER_CHAIN h
        ON h.CHAIN = 'Arbitrum'
        AND LLAMA_NAME != ''
        AND h.PROTOCOL_NAME LIKE LLAMA_NAME || '%'
        AND DATE < DATE_TRUNC('{time}',CURRENT_DATE())
        AND DATE >= to_timestamp('{start_month}', 'yyyy-MM-dd')
        AND m.CHAIN = 'Arbitrum One'
        AND m.NAME NOT IN ({exclude_list})
    )
    WHERE DATE >= CASE
        WHEN TRY_TO_TIMESTAMP(GRANT_DATE, 'MM/DD/YYYY') IS NOT NULL THEN TRY_TO_TIMESTAMP(GRANT_DATE, 'MM/DD/YYYY')
        ELSE TO_TIMESTAMP('2023-03-01', 'YYYY-MM-DD')
    END
    AND rn = 1
    GROUP BY 1
    )

    , prices AS (
    SELECT 
    DATE_TRUNC('{time}',HOUR) AS date,
    LAST_VALUE(USD_PRICE) OVER (PARTITION BY DATE_TRUNC('{time}', HOUR) ORDER BY HOUR) AS USD_PRICE
    FROM COMMON.PRICES.TOKEN_PRICES_HOURLY_EASY
    WHERE SYMBOL = 'ETH'
    AND ETHEREUM_ADDRESS = '0x0000000000000000000000000000000000000000'
    AND HOUR >= to_timestamp('{start_month}', 'yyyy-MM-dd')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('{time}', HOUR) ORDER BY HOUR DESC) = 1
    )

    SELECT 
    m.DATE,
    m.TVL,
    m.TVL/p.USD_PRICE AS TVL_ETH
    FROM grantees m
    LEFT JOIN prices p
    ON m.DATE = p.DATE
    ''',
                                       time=timeframe,
                                       exclude_list=exclude_list,
                                       start_month=start_month)

    tvl_chart_post_grant = [{
      k: v
      for k, v in item.items() if k != 'TVL_ETH'
    } for item in tvl_post_grant_query]
    tvl_chart_eth_post_grant = [{
      k: v
      for k, v in item.items() if k != 'TVL'
    } for item in tvl_post_grant_query]

    accounts_chart_post_grant = execute_sql('''
    SELECT 
    TO_VARCHAR(DATE_TRUNC('{time}',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
    COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
    FROM ARBITRUM.RAW.TRANSACTIONS t
    INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
    ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
    AND BLOCK_TIMESTAMP < DATE_TRUNC('{time}',CURRENT_DATE())
    AND BLOCK_TIMESTAMP >= to_timestamp('{start_month}', 'yyyy-MM-dd')
    INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
    ON c.NAME = m.NAME
    AND t.BLOCK_TIMESTAMP >= CASE
        WHEN TRY_TO_TIMESTAMP(m.GRANT_DATE, 'MM/DD/YYYY') IS NOT NULL THEN TRY_TO_TIMESTAMP(m.GRANT_DATE, 'MM/DD/YYYY')
        ELSE TO_TIMESTAMP('2023-03-01', 'YYYY-MM-DD')
    END
    AND m.CHAIN = 'Arbitrum One'
    AND m.NAME NOT IN ({exclude_list})
    GROUP BY 1
    ''',
                                            time=timeframe,
                                            exclude_list=exclude_list,
                                            start_month=start_month)

    tvl_pie = execute_sql('''
    WITH cte AS (
      SELECT 
        NAME,
        TVL,
        SUM(TVL) OVER () AS TOTAL_TVL
      FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_DAY_TVL_BY_PROJECT
      WHERE DATE = TO_VARCHAR(DATE_TRUNC('day',CURRENT_DATE - INTERVAL '1 DAY'), 'YYYY-MM-DD')
      AND NAME NOT IN ({exclude_list})
    ),
    ranked_cte AS (
      SELECT 
        NAME,
        TVL,
        TOTAL_TVL,
        ROUND(TVL / TOTAL_TVL * 100, 2) AS PCT_TVL,
        RANK() OVER (ORDER BY TVL DESC) AS rnk
      FROM cte
    )
    SELECT 
      CASE WHEN rnk <= 5 THEN NAME ELSE 'Other' END AS NAME,
      SUM(TVL) AS TVL,
      ROUND(SUM(TVL) / MAX(TOTAL_TVL) * 100, 2) AS PCT_TVL
    FROM ranked_cte
    GROUP BY CASE WHEN rnk <= 5 THEN NAME ELSE 'Other' END
    ORDER BY TVL DESC
    ''',
                          exclude_list=exclude_list)

    accounts_pie = execute_sql('''
    WITH cte AS (
    SELECT 
    c.NAME,
    COUNT(DISTINCT FROM_ADDRESS) AS active_wallets,
    SUM(COUNT(DISTINCT FROM_ADDRESS)) OVER () AS total_wallets
    FROM ARBITRUM.RAW.TRANSACTIONS t
    INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
    ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
    AND BLOCK_TIMESTAMP < CURRENT_DATE
    AND BLOCK_TIMESTAMP >= CURRENT_DATE - interval '{time_param}'
    AND C.NAME NOT IN ({exclude_list})
    INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
    ON m.NAME = c.NAME
    AND m.CHAIN = 'Arbitrum One'
    GROUP BY 1
    ),
    ranked_cte AS (
      SELECT 
        NAME,
        active_wallets,
        total_wallets,
        ROUND(active_wallets / total_wallets * 100, 2) AS PCT_TVL,
        RANK() OVER (ORDER BY active_wallets DESC) AS rnk
      FROM cte
    )
    SELECT 
      CASE WHEN rnk <= 5 THEN NAME ELSE 'Other' END AS NAME,
      SUM(active_wallets) AS active_wallets,
      ROUND(SUM(active_wallets) / MAX(total_wallets) * 100, 2) AS PCT_WALLETS
    FROM ranked_cte
    GROUP BY CASE WHEN rnk <= 5 THEN NAME ELSE 'Other' END
    ORDER BY active_wallets DESC
    ''',
                               time_param=time_param,
                               time=timeframe,
                               exclude_list=exclude_list)

    leaderboard = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_LEADERBOARD
    UNION ALL
    SELECT
        'TOTAL' as project,
        'Total' as category,
        'total' as slug,
        'https://aefsitlkirjpwxayubwd.supabase.co/storage/v1/object/public/Arbigrants%20logos/AF_logomark.png?t=2024-07-03T10%3A53%3A13.645Z' as logo,
        '' as chain,
        SUM(ETH_FEES) as ETH_FEES,
        0 as ETH_FEES_GROWTH,
        SUM(TRANSACTIONS) as TRANSACTIONS,
        0 as TRANSACTIONS_GROWTH,
        SUM(WALLETS) as WALLETS,
        0 as WALLETS_GROWTH,
        SUM(tvl) as tvl,
        SUM(volume) as volume
    FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_LEADERBOARD
    ORDER BY WALLETS DESC
    ''',
                              time=timeframe)

    milestones = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ALL_MILESTONE_SUMMARY
    ''')

    name_list = execute_sql('''
    SELECT NAME FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA
    ''')

    current_time = datetime.now().strftime('%d/%m/%y %H:%M')

    response_data = {
      "time": current_time,
      "wallets_stat": wallets_stat,
      "wallets_pct_stat": wallets_pct_stat,
      "tvl_stat": tvl_stat,
      "tvl_pct_stat": tvl_pct_stat,
      "gas_stat": gas_stat,
      "gas_pct_stat": gas_pct_stat,
      "tvl_chart": tvl_chart,
      "tvl_chart_eth": tvl_chart_eth,
      "accounts_chart": accounts_chart,
      "tvl_chart_post_grant": tvl_chart_post_grant,
      "tvl_chart_eth_post_grant": tvl_chart_eth_post_grant,
      "accounts_chart_post_grant": accounts_chart_post_grant,
      "tvl_pie": tvl_pie,
      "accounts_pie": accounts_pie,
      "leaderboard": leaderboard,
      "milestones": milestones,
      "name_list": name_list,
    }

    return jsonify(response_data)


@app.route('/grantee')
@cache.memoize(make_name=make_cache_key)
def entity():
  timeframe = request.args.get('timeframe', 'week')
  grantee_name = request.args.get('grantee_name', 'pendle')

  info = execute_sql('''
  SELECT 
  NAME,
  LOGO,
  DESCRIPTION,
  WEBSITE,
  TWITTER,
  DUNE
  FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA
  WHERE NAME = '{grantee_name}'
  ''',
                     grantee_name=grantee_name)

  wallets_chart = execute_sql('''
  SELECT 
  DATE,
  ACTIVE_WALLETS
  FROM ARBIGRANTS.DBT.ARBIGRANTS_ALL_{time}_ACTIVE_WALLETS_BY_PROJECT
  WHERE NAME = '{grantee_name}'
  ORDER BY 1
  ''',
                              time=timeframe,
                              grantee_name=grantee_name)

  gas_chart = execute_sql('''
  SELECT 
  DATE,
  GAS_SPEND
  FROM ARBIGRANTS.DBT.ARBIGRANTS_ALL_{time}_GAS_SPEND_BY_PROJECT
  WHERE NAME = '{grantee_name}'
  ORDER BY 1
  ''',
                          time=timeframe,
                          grantee_name=grantee_name)

  txns_chart = execute_sql('''
  SELECT 
  TO_VARCHAR(DATE_TRUNC('{time}',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
  COUNT(*) AS transactions
  FROM ARBITRUM.RAW.TRANSACTIONS t
  INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
  ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
  AND t.BLOCK_TIMESTAMP < DATE_TRUNC('{time}',CURRENT_DATE())
  AND t.BLOCK_TIMESTAMP >= to_timestamp('2023-06-01', 'yyyy-MM-dd')
  AND c.NAME = '{grantee_name}'
  GROUP BY 1
  ORDER BY 1
  ''',
                           time=timeframe,
                           grantee_name=grantee_name)

  llama_bool = execute_sql('''
  SELECT 
  CASE WHEN LLAMA_NAME <> '' THEN 1
  ELSE 0
  END AS LLAMA_COUNT
  FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA
  WHERE NAME = '{grantee_name}'
  ''',
                           grantee_name=grantee_name)

  if llama_bool[0]["LLAMA_COUNT"] == 0:
    tvl_chart = 0
  else:
    tvl_chart = execute_sql('''
    SELECT 
    DATE,
    TVL
    FROM ARBIGRANTS.DBT.ARBIGRANTS_ALL_{time}_TVL_BY_PROJECT
    WHERE NAME = '{grantee_name}'
    ORDER BY 1
    ''',
                            time=timeframe,
                            grantee_name=grantee_name)

  grant_date_bool = execute_sql('''
  SELECT 
  CASE WHEN GRANT_DATE <> '' THEN 1
  ELSE 0
  END AS GRANT_DATE_COUNT
  FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA
  WHERE NAME = '{grantee_name}'
  ''',
                                grantee_name=grantee_name)

  if grant_date_bool[0]["GRANT_DATE_COUNT"] == 0:
    grant_date = 0
  else:
    grant_date = execute_sql('''
    SELECT GRANT_DATE
    FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA
    WHERE NAME = '{grantee_name}'
    ''',
                             grantee_name=grantee_name)

  milestones = execute_sql('''
  SELECT MILESTONES_COMPLETED, TOTAL_MILESTONES
  FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_MILESTONES
  WHERE NAME = '{grantee_name}'
  ''',
                           grantee_name=grantee_name)

  response_data = {
    "info": info,
    "wallets_chart": wallets_chart,
    "gas_chart": gas_chart,
    "txns_chart": txns_chart,
    "tvl_chart": tvl_chart,
    "llama_bool": llama_bool,
    "grant_date_bool": grant_date_bool,
    "grant_date": grant_date,
    "milestones": milestones
  }

  return jsonify(response_data)


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=81)
