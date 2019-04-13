import concurrent.futures
import threading
import time

from datetime import datetime, timezone
import json

import redis

from pybitmex import *

from bitmex_order_dispatcher.settings import settings
from bitmex_order_dispatcher.utils import log


logger = log.setup_custom_logger('server')


NUM_DISPATCHER_WORKERS = settings.NUM_DISPATCHER_WORKERS

REDIS_POP_TIMEOUT_SECONDS = settings.REDIS_POP_TIMEOUT_SECONDS

REDIS_CANCEL_QUEUE_NAME = settings.REDIS_CANCEL_QUEUE_NAME
REDIS_POST_ONLY_ORDER_QUEUE_NAME = settings.REDIS_POST_ONLY_ORDER_QUEUE_NAME

REST_MAX_RETRIES = settings.REST_MAX_RETRIES


class Instruction:

    def __init__(self, is_order, json_obj, is_post_only=True):
        self.is_order = is_order
        self.is_cancel = not self.is_order
        self.is_post_only = is_post_only
        if self.is_order:
            self.order_list = json_obj
            self.cancel_id_list = None
        else:
            self.order_list = None
            self.cancel_id_list = json_obj

    def get_instruction_type_str(self):
        if self.is_order:
            return "Order"
        elif self.is_cancel:
            return "Cancel"
        else:
            return "?"


class OrderDispatcher:

    def __init__(self):
        # WS and REST client.
        self.bitmex_client = self._create_bitmex_client()
        # Redis client.
        self.redis = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)

    @staticmethod
    def _create_bitmex_client():
        return BitMEXClient(
            settings.BASE_URL, settings.SYMBOL,
            api_key=settings.API_KEY, api_secret=settings.API_SECRET,
            use_websocket=False, use_rest=True,
            subscriptions=[],
            order_id_prefix=settings.ORDER_ID_PREFIX,
            agent_name=settings.AGENT_NAME,
            http_timeout=settings.REST_TIMEOUT_SECONDS,
            expiration_seconds=settings.API_EXPIRATION_SECONDS
        )

    def _refresh_bitmex_client(self, thread_name, sleep_seconds=0):
        logger.info("Refreshing BitMEX client on %s", thread_name)
        self.bitmex_client.close()
        if 0 < sleep_seconds:
            time.sleep(sleep_seconds)
        self.bitmex_client = self._create_bitmex_client()

    @staticmethod
    def _parse_instruction(instruction):
        if instruction:
            try:
                key = instruction[0].decode('utf-8')
                json_obj = json.loads(instruction[1].decode('utf-8'))
                if key == REDIS_CANCEL_QUEUE_NAME:
                    return Instruction(is_order=False, json_obj=json_obj)
                elif key == REDIS_POST_ONLY_ORDER_QUEUE_NAME:
                    return Instruction(is_order=True, json_obj=json_obj, is_post_only=True)
                else:
                    logger.info("Unknown key: %s", key)
                    return None
            except Exception as e:
                # Error in parsing. Should NOT affect the thread / process life.
                logger.info(str(e))
                return None
        else:
            return None

    @staticmethod
    def _now():
        return datetime.now().astimezone(timezone.utc)

    def read_and_dispatch_instructions(self):
        try:
            while True:
                thread_name = threading.current_thread().name
                # Blocking wait for a new instruction to arrive.
                instruction = self.redis.brpop(
                    [REDIS_CANCEL_QUEUE_NAME, REDIS_POST_ONLY_ORDER_QUEUE_NAME],
                    timeout=REDIS_POP_TIMEOUT_SECONDS
                )
                parsed_instruction = self._parse_instruction(instruction)
                if parsed_instruction:
                    each_start_time = self._now()
                    rest_end_time = each_start_time
                    is_success = False
                    code = 0
                    try:
                        if parsed_instruction.is_cancel:
                            logger.info("Received cancel id list %s on %s",
                                        str(parsed_instruction.cancel_id_list), thread_name)
                            self.bitmex_client.rest_cancel_orders(
                                parsed_instruction.cancel_id_list,
                                max_retries=REST_MAX_RETRIES
                            )
                            logger.info("Cancellation requested.")
                        elif parsed_instruction.is_order:
                            logger.info("Received order list %s on %s",
                                        str(parsed_instruction.order_list), thread_name)
                            self.bitmex_client.rest_place_orders(
                                parsed_instruction.order_list,
                                post_only=parsed_instruction.is_post_only,
                                max_retries=REST_MAX_RETRIES
                            )
                            logger.info("Order placed.")
                        is_success = True
                        rest_end_time = self._now()
                    except RestClientError as ex:
                        rest_end_time = self._now()
                        code = ex.error_code
                        if ex.is_4xx():
                            logger.warning("HTTP %d ERROR", ex.error_code)
                            logger.info(ex.message_str)
                            self._refresh_bitmex_client(thread_name)
                        elif ex.is_5xx():
                            logger.info(ex.message_str)
                            self._refresh_bitmex_client(thread_name, 1)
                        elif ex.is_timeout():
                            self._refresh_bitmex_client(thread_name, 1)
                        else:
                            logger.warning("Communication Error? %d", ex.error_code)
                            logger.info(ex.message_str)
                            self._refresh_bitmex_client(thread_name, 1)
                    except Exception as e:
                        rest_end_time = self._now()
                        code = -1
                        import sys
                        import traceback
                        traceback.print_exc(file=sys.stdout)

                        logger.warning("Error: %s" % str(e))
                        logger.info(sys.exc_info())

                        self._refresh_bitmex_client(thread_name, 1)
                    finally:
                        each_end_time = self._now()
                        rest_elapsed_seconds = (rest_end_time - each_start_time).total_seconds()
                        total_elapsed_seconds = (each_end_time - each_start_time).total_seconds()
                        logger.info(
                            "(SUMMARY) Type: %s, RestElapsed: %.1f, Elapsed: %.1f, Success: %s, Code: %d, Thread: %s",
                            parsed_instruction.get_instruction_type_str(),
                            rest_elapsed_seconds, total_elapsed_seconds, str(is_success), code, thread_name
                        )
                else:
                    logger.debug("Redis queue timeout or parsing problem on %s", thread_name)
        finally:
            self.bitmex_client.close()


def start():
    from bitmex_order_dispatcher.utils import constants

    logger.info('STARTING BitMEX Order Dispatcher. Version=%s. Workers=%d',
                constants.VERSION, settings.NUM_DISPATCHER_WORKERS)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=NUM_DISPATCHER_WORKERS, thread_name_prefix="D")
    for _ in range(NUM_DISPATCHER_WORKERS):
        dispatcher = OrderDispatcher()
        executor.submit(dispatcher.read_and_dispatch_instructions)
