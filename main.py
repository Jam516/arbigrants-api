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
  "CACHE_DEFAULT_TIMEOUT": 600,
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

  txns_stat = execute_sql('''
  SELECT {time}_TRANSACTIONS AS TRANSACTIONS FROM 
  ARBIGRANTS.DBT.ARBIGRANTS_ONE_SUMMARY
  ''',
                          time=timeframe)

  txns_pct_stat = execute_sql('''
  SELECT PCT_{time}_TRANSACTIONS AS PCT_TRANSACTIONS FROM 
  ARBIGRANTS.DBT.ARBIGRANTS_ONE_SUMMARY
  ''',
                              time=timeframe)

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

  gas_spend_chart = execute_sql('''
  SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_GAS_SPEND
  WHERE DATE >= '2024-01-01'
  ORDER BY DATE
  ''',
                                time=timeframe)

  accounts_chart = execute_sql('''
  SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_ACTIVE_WALLETS
  WHERE DATE >= '2024-01-01'
  ORDER BY DATE 
  ''',
                               time=timeframe)

  leaderboard = execute_sql('''
  SELECT * FROM ARBIGRANTS.DBT.ARBIGRANTS_ONE_{time}_LEADERBOARD
  ORDER BY ETH_FEES DESC 
  ''',
                            time=timeframe)

  current_time = datetime.now().strftime('%d/%m/%y %H:%M')

  response_data = {
    "time": current_time,
    "wallets_stat": wallets_stat,
    "wallets_pct_stat": wallets_pct_stat,
    "txns_stat": txns_stat,
    "txns_pct_stat": txns_pct_stat,
    "gas_stat": gas_stat,
    "gas_pct_stat": gas_pct_stat,
    "gas_spend_chart": gas_spend_chart,
    "accounts_chart": accounts_chart,
    "leaderboard": leaderboard
  }

  return jsonify(response_data)


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=81)
