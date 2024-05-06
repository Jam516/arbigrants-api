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
  SELECT DAY_ACTIVE_WALLETS FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_ACTIVE_WALLET_SUMMARY
  ''')

  actives_growth_24h = execute_sql('''
  SELECT DAY_GROWTH FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_ACTIVE_WALLET_SUMMARY
  ''')

  actives_7d = execute_sql('''
  SELECT WEEK_ACTIVE_WALLETS FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_ACTIVE_WALLET_SUMMARY
  ''')

  actives_growth_7d = execute_sql('''
  SELECT WEEK_GROWTH FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_ACTIVE_WALLET_SUMMARY
  ''')

  actives_1m = execute_sql('''
  SELECT MONTH_ACTIVE_WALLETS FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_ACTIVE_WALLET_SUMMARY
  ''')

  actives_growth_1m = execute_sql('''
  SELECT MONTH_GROWTH FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_ACTIVE_WALLET_SUMMARY
  ''')

  gas_spend_chart = execute_sql('''
  SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_GAS_SPEND
  WHERE DATE > '2024-01-01'
  ORDER BY DATE
  ''',
                                time=timeframe)

  accounts_chart = execute_sql('''
  SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_ACTIVE_WALLETS
  WHERE DATE > '2024-01-01'
  ORDER BY DATE 
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
