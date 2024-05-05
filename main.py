from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_caching import Cache
from httpx import Timeout
import snowflake.connector
from snowflake.connector import DictCursor
from datetime import datetime
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
  "CACHE_DEFAULT_TIMEOUT": 60,
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


LLAMA_API = "https://api.llama.fi"

# async def get_llama_data(endpoint):
#   timeout = Timeout(40.0)
#   async with httpx.AsyncClient(timeout=timeout) as client:
#     try:
#       response = await client.get(f'{LLAMA_API}/{endpoint}')
#       if response.status_code != 200:
#         app.logger.error(f"Failed to get data from llama API: {response.text}")
#         return None, response.status_code
#       return response.json(), response.status_code
#     except httpx.HTTPError as ex:
#       app.logger.error(
#         f"Exception occurred while calling llama API: {type(ex).__name__}, {ex.args}"
#       )
#       return None, 500

# async def get_tvls(slugs, slugs_dict):
#   # Create a map from slug to dictionary for easy update
#   slug_map = {d['SLUG']: d for d in slugs_dict}

#   async def fetch_tvl(slug):
#     response_data, status_code = await get_llama_data(f'protocol/{slug}')
#     if response_data is None:
#       return slug, None
#     return slug, response_data.get('currentChainTvls', {}).get('ARBITRUM', None)

#   # Gather all tasks
#   tasks = [fetch_tvl(slug) for slug in slugs]
#   results = await asyncio.gather(*tasks)

#   # Update slugs_dict with the fetched TVL values
#   for slug, tvl in results:
#     if slug in slug_map:
#       slug_map[slug]['TVL'] = tvl

#   return list(slug_map.values())


@app.route('/overview')
@cache.memoize(make_name=make_cache_key)
def overview():
  timeframe = request.args.get('timeframe', 'month')

  actives_24h = execute_sql('''
  SELECT COUNT(DISTINCT FROM_ADDRESS) as active_wallets
  FROM ARBITRUM.RAW.TRANSACTIONS t
  INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
  ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
  AND BLOCK_TIMESTAMP >= current_timestamp - interval '1 day' 
  ''')

  actives_growth_24h = execute_sql('''
  WITH active_wallet_counts AS (
      SELECT
          COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP >= current_timestamp() - interval '1 day' THEN FROM_ADDRESS END) as past_day_wallets,
          COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP < current_timestamp() - interval '1 day' AND BLOCK_TIMESTAMP >= current_timestamp() - interval '2 day' THEN FROM_ADDRESS END) as day_before_wallets
      FROM ARBITRUM.RAW.TRANSACTIONS t
      INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
      ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
      AND BLOCK_TIMESTAMP >= current_timestamp() - interval '2 day'
  )
  SELECT
      ROUND((100 * (past_day_wallets / NULLIF(day_before_wallets, 0)) - 100), 1) AS daily_growth
  FROM active_wallet_counts;
  ''')

  actives_7d = execute_sql('''
  SELECT COUNT(DISTINCT FROM_ADDRESS) as active_wallets
  FROM ARBITRUM.RAW.TRANSACTIONS t
  INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
  ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
  AND BLOCK_TIMESTAMP >= current_timestamp - interval '7 day'
  ''')

  actives_growth_7d = execute_sql('''
  WITH active_wallet_counts AS (
      SELECT
          COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP >= current_timestamp() - interval '7 day' THEN FROM_ADDRESS END) as past_week_wallets,
          COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP < current_timestamp() - interval '7 day' AND BLOCK_TIMESTAMP >= current_timestamp() - interval '14 day' THEN FROM_ADDRESS END) as week_before_wallets
      FROM ARBITRUM.RAW.TRANSACTIONS t
      INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
      ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
      AND BLOCK_TIMESTAMP >= current_timestamp() - interval '14 day'
  )
  SELECT
      ROUND((100 * (past_week_wallets / NULLIF(week_before_wallets, 0)) - 100), 1) AS weekly_growth
  FROM active_wallet_counts;
  ''')

  actives_1m = execute_sql('''
  SELECT COUNT(DISTINCT FROM_ADDRESS) as active_wallets 
  FROM ARBITRUM.RAW.TRANSACTIONS t
  INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
  ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
  AND BLOCK_TIMESTAMP >= current_timestamp - interval '1 month' 
  ''')

  actives_growth_1m = execute_sql('''
  WITH active_wallet_counts AS (
      SELECT
          COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP >= current_timestamp() - interval '1 month' THEN FROM_ADDRESS END) as past_month_wallets,
          COUNT(DISTINCT CASE WHEN BLOCK_TIMESTAMP < current_timestamp() - interval '1 month' AND BLOCK_TIMESTAMP >= current_timestamp() - interval '2 months' THEN FROM_ADDRESS END) as month_before_wallets
      FROM ARBITRUM.RAW.TRANSACTIONS t
      INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
      ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
      AND BLOCK_TIMESTAMP >= current_timestamp() - interval '2 months'
  )
  SELECT
      ROUND((100 * (past_month_wallets / NULLIF(month_before_wallets, 0)) - 100), 1) AS monthly_growth
  FROM active_wallet_counts;
  ''')

  gas_spend_chart = execute_sql('''
  with total AS (
  SELECT 
  DATE_TRUNC('{time}',BLOCK_TIMESTAMP) AS date,
  'total' as category,
  SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS gas_spend
  FROM ARBITRUM.RAW.TRANSACTIONS
  WHERE BLOCK_TIMESTAMP > '2023-01-01'
  GROUP BY 1,2
  )
  
  , grantees AS (
  SELECT 
  DATE_TRUNC('{time}',BLOCK_TIMESTAMP) AS date,
  'grantees' as category,
  SUM((RECEIPT_EFFECTIVE_GAS_PRICE * RECEIPT_GAS_USED)/1e18) AS gas_spend_grantees
  FROM ARBITRUM.RAW.TRANSACTIONS t
  INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
  ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
  AND BLOCK_TIMESTAMP > '2023-01-01'
  GROUP BY 1,2
  )
  
  SELECT *
  FROM total
  UNION ALL SELECT *
  FROM grantees
  ''',
                                time=timeframe)

  accounts_chart = execute_sql('''
  with total AS (
  SELECT 
  DATE_TRUNC('{time}',BLOCK_TIMESTAMP) AS date,
  'total' as category,
  COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
  FROM ARBITRUM.RAW.TRANSACTIONS
  WHERE BLOCK_TIMESTAMP > '2023-01-01'
  GROUP BY 1,2
  )
  
  , grantees AS (
  SELECT 
  DATE_TRUNC('{time}',BLOCK_TIMESTAMP) AS date,
  'grantees' as category,
  COUNT(DISTINCT FROM_ADDRESS) AS active_wallets
  FROM ARBITRUM.RAW.TRANSACTIONS t
  INNER JOIN ARBIGRANTS.DBT.ARBIGRANTS_LABELS_PROJECT_CONTRACTS c
  ON c.CONTRACT_ADDRESS = t.TO_ADDRESS
  AND BLOCK_TIMESTAMP > '2023-01-01'
  GROUP BY 1,2
  )
  
  SELECT * FROM total
  UNION ALL 
  SELECT * FROM grantees
  ''',
                               time=timeframe)

  current_time = datetime.now().strftime('%d/%m/%y %H:%M')

  response_data = {
    "time": current_time,
    "actives_24h": actives_24h,
    "actives_growth_24h": actives_growth_24h,
    "actives_7d": actives_7d,
    "actives_growth_7d": actives_growth_7d,
    "actives_1m": actives_1m,
    "actives_growth_1m": actives_growth_1m,
    "gas_spend_chart": gas_spend_chart,
    "accounts_chart": accounts_chart,
  }

  return jsonify(response_data)


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=81)
