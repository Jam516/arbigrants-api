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
  "CACHE_DEFAULT_TIMEOUT": 7200,
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
                                     database="BUNDLEBEAR",
                                     schema="ERC4337")

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

  excludes = request.args.getlist('excludes')
  exclude_list = ",".join(f"'{item}'" for item in excludes) if excludes else ""

  current_date = datetime.now()
  previous_month = current_date.replace(day=1) - relativedelta(
    months=timescale)
  start_month = previous_month.strftime('%Y-%m-%d')

  if exclude_list == "":
    wallets_stat = execute_sql('''
    SELECT {time}_ACTIVE_WALLETS AS ACTIVE_WALLETS FROM 
    ARBIGRANTS.DBT.ARBIGRANTS_ONE_SUMMARY
    ''',
                               time=timeframe)

    wallets_pct_stat = execute_sql('''
    SELECT PCT_{time}_ACTIVE_WALLETS AS PCT_WALLETS FROM     
    ARBIGRANTS.DBT.ARBIGRANTS_ONE_SUMMARY
    ''',
                                   time=timeframe)

    tvl_stat = execute_sql('''
    SELECT TVL_GRANTEES FROM 
    ARBIGRANTS.DBT.ARBIGRANTS_ONE_SUMMARY
    ''')

    tvl_pct_stat = execute_sql('''
    SELECT PCT_TVL FROM 
    ARBIGRANTS.DBT.ARBIGRANTS_ONE_SUMMARY
    ''')

    gas_stat = execute_sql('''
    SELECT {time}_GAS_SPEND AS GAS_SPEND FROM     
    ARBIGRANTS.DBT.ARBIGRANTS_ONE_SUMMARY
    ''',
                           time=timeframe)

    gas_pct_stat = execute_sql('''
    SELECT PCT_{time}_GAS_SPEND AS PCT_GAS_SPEND FROM 
    ARBIGRANTS.DBT.ARBIGRANTS_ONE_SUMMARY
    ''',
                               time=timeframe)

    # gas_spend_chart = execute_sql('''
    # SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_GAS_SPEND
    # WHERE DATE >= '2024-01-01'
    # ORDER BY DATE
    # ''',
    #                               time=timeframe)

    tvl_chart = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_TVL
    WHERE DATE >= '{start_month}'
    ORDER BY DATE
    ''',
                            time=timeframe,
                            start_month=start_month)

    accounts_chart = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_ACTIVE_WALLETS
    WHERE DATE >= '{start_month}'
    ORDER BY DATE 
    ''',
                                 time=timeframe,
                                 start_month=start_month)

    tvl_pie = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_TVL_PIE
    ''')

    accounts_pie = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_WALLETS_PIE
    ''',
                               time=timeframe)

    leaderboard = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_LEADERBOARD
    ORDER BY ETH_FEES DESC 
    ''',
                              time=timeframe)

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
      # "gas_spend_chart": gas_spend_chart,
      "tvl_chart": tvl_chart,
      "accounts_chart": accounts_chart,
      "tvl_pie": tvl_pie,
      "accounts_pie": accounts_pie,
      "leaderboard": leaderboard,
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
    AND h.DATE = current_date
    AND h.PROTOCOL_NAME = LLAMA_NAME
    AND m.NAME NOT IN ({exclude_list})
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

    tvl_chart = execute_sql('''
    with total AS (
    SELECT 
    TO_VARCHAR(DATE_TRUNC('{time}',DATE), 'YYYY-MM-DD') AS date,
    'total' as category,
    LAST_VALUE(TVL) OVER (PARTITION BY DATE_TRUNC('{time}', DATE) ORDER BY DATE) AS TVL
    FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_TOTAL_TVL
    WHERE DATE < DATE_TRUNC('day',CURRENT_DATE())
    AND DATE >= to_timestamp('{start_month}', 'yyyy-MM-dd')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('{time}', DATE) ORDER BY DATE DESC) = 1
    )

    , grantees AS (
    SELECT 
    TO_VARCHAR(DATE_TRUNC('{time}',DATE), 'YYYY-MM-DD') AS date,
    category,
    LAST_VALUE(TVL) OVER (PARTITION BY DATE_TRUNC('{time}', DATE) ORDER BY DATE) AS TVL
    FROM (
    SELECT 
    DATE,
    'grantees' as category,
    SUM(h.TOTAL_LIQUIDITY_USD) AS TVL
    FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
    INNER JOIN DEFILLAMA.TVL.HISTORICAL_TVL_PER_CHAIN h
    ON h.CHAIN = 'Arbitrum'
    AND h.PROTOCOL_NAME = LLAMA_NAME
    AND DATE < DATE_TRUNC('day',CURRENT_DATE())
    AND DATE >= to_timestamp('{start_month}', 'yyyy-MM-dd')
    AND m.NAME NOT IN ({exclude_list})
    GROUP BY 1,2)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('{time}', DATE) ORDER BY DATE DESC) = 1
    )

    SELECT * FROM total
    UNION ALL 
    SELECT * FROM grantees
    ORDER BY DATE
    ''',
                            time=timeframe,
                            start_month=start_month,
                            exclude_list=exclude_list)

    accounts_chart = execute_sql('''
    with total AS (
    SELECT 
    TO_VARCHAR(DATE_TRUNC('{time}',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
    'total' as category,
    COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
    FROM ARBITRUM.RAW.TRANSACTIONS
    WHERE BLOCK_TIMESTAMP < DATE_TRUNC('{time}',CURRENT_DATE())
    AND BLOCK_TIMESTAMP >= to_timestamp('{start_month}', 'yyyy-MM-dd')
    GROUP BY 1,2
    )

    , grantees AS (
    SELECT 
    TO_VARCHAR(DATE_TRUNC('{time}',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
    'grantees' as category,
    COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
    FROM ARBITRUM.RAW.TRANSACTIONS t
    INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
    ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
    AND BLOCK_TIMESTAMP < DATE_TRUNC('{time}',CURRENT_DATE())
    AND BLOCK_TIMESTAMP >= to_timestamp('{start_month}', 'yyyy-MM-dd')
    AND c.NAME NOT IN ({exclude_list})
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

    tvl_pie = execute_sql('''
    WITH cte AS (
      SELECT 
        m.NAME,
        h.TOTAL_LIQUIDITY_USD AS TVL,
        SUM(h.TOTAL_LIQUIDITY_USD) OVER () AS TOTAL_TVL
      FROM ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
      INNER JOIN DEFILLAMA.TVL.HISTORICAL_TVL_PER_CHAIN h
      ON h.CHAIN = 'Arbitrum'
        AND h.PROTOCOL_NAME = LLAMA_NAME
        AND DATE = CURRENT_DATE
        AND m.NAME NOT IN ({exclude_list})
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
    ''',time_param=time_param,
                               time=timeframe,
                              exclude_list=exclude_list)

    leaderboard = execute_sql('''
    SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_LEADERBOARD
    ORDER BY ETH_FEES DESC 
    ''',
                              time=timeframe)

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
      "accounts_chart": accounts_chart,
      "tvl_pie": tvl_pie,
      "accounts_pie": accounts_pie,
      "leaderboard": leaderboard,
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
  TO_VARCHAR(DATE_TRUNC('{time}',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
  COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
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

  gas_chart = execute_sql('''
  SELECT 
  TO_VARCHAR(DATE_TRUNC('{time}',BLOCK_TIMESTAMP), 'YYYY-MM-DD') AS date,
  SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS gas_spend
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
        TO_VARCHAR(DATE_TRUNC('{time}',DATE), 'YYYY-MM-DD') AS date,
        TOTAL_LIQUIDITY_USD as TVL
    FROM DEFILLAMA.TVL.HISTORICAL_TVL_PER_CHAIN tv
    INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_METADATA m
        ON tv.PROTOCOL_NAME = m.LLAMA_NAME
        AND DATE >= TO_TIMESTAMP('2023-06-01', 'yyyy-MM-dd')
        AND tv.CHAIN = 'Arbitrum'
        AND m.NAME = '{grantee_name}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('week', DATE) ORDER BY DATE ASC) = 1
    ORDER BY 1;
    ''',
                            time=timeframe,
                            grantee_name=grantee_name)

  response_data = {
    "info": info,
    "wallets_chart": wallets_chart,
    "gas_chart": gas_chart,
    "txns_chart": txns_chart,
    "tvl_chart": tvl_chart,
    "llama_bool": llama_bool
  }

  return jsonify(response_data)


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=81)
