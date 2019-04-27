import logging
import os

########################################################################################################################
# Connection and authentication to the exchange and middle wares.
########################################################################################################################

# API URL.
# BASE_URL = "https://testnet.bitmex.com/api/v1/"
BASE_URL = "https://www.bitmex.com/api/v1/"

API_KEY = os.environ['BITMEX_API_KEY']
API_SECRET = os.environ['BITMEX_API_SECRET']

REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 0

REDIS_CANCEL_QUEUE_NAME = 'to-dispatcher:cancels'
REDIS_POST_ONLY_ORDER_QUEUE_NAME = 'to-dispatcher:post-only-orders'
REDIS_MARKET_CLOSE_ORDER_QUEUE_NAME = 'to-dispatcher:market-close-orders'

REDIS_POP_TIMEOUT_SECONDS = 5

GRAPHITE_HOST = "graphite"
GRAPHITE_PORT = 2004

########################################################################################################################
# Target
########################################################################################################################

# Instrument to market make on BitMEX.
SYMBOL = "XBTUSD"

########################################################################################################################
# Misc Behavior
########################################################################################################################

INITIAL_SLEEP_SECONDS = 0

API_EXPIRATION_SECONDS = 3600

REST_TIMEOUT_SECONDS = 10
REST_MAX_RETRIES = 2

ORDER_ID_PREFIX = "order_dispatcher_"
AGENT_NAME = "bmx-order-dispatcher"

NUM_DISPATCHER_WORKERS = int(os.environ['NUM_DISPATCHER_WORKERS'])

########################################################################################################################
# Logging
########################################################################################################################
# Available levels: logging.(DEBUG|INFO|WARN|ERROR)
LOG_LEVEL = logging.INFO

# Logging to files is not recommended when you run this on Docker.
# By leaving the name empty the program avoids to create log files.
LOG_FILE_NAME = ''
