# etl_utils.py
"""

"""
import logging
from numbers import Integral
from time import sleep
from typing import MutableMapping, Callable

from requests import Response, HTTPError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s: %(name)s - [%(levelname)s] - %(message)s'
)


# TODO implement exponential backoff.

class APIRetry:
    def __init__(
            self,
            http_code_handlers: MutableMapping[int, Callable] = None,
            backoff: Integral = 1,
            max_wait: Integral = 10):
        self.http_code_handlers = http_code_handlers
        self.backoff = backoff
        self.max_wait = max_wait

    def __call__(self, req_func):
        """
        Wraps the passed in function in a function that will

        :param req_func:
        :param args:
        :param kwargs:
        :return:
        """
        logger.debug(f'Inside __call__, received: {req_func}')

        def wrap(*args, **kwargs):
            logger.debug(f'Received: {args} - {kwargs}')
            wait = 0
            latest_exc = None

            while wait < self.max_wait:
                sleep(wait)
                resp: Response = req_func(*args, **kwargs)
                try:
                    logger.debug(f'Response: {resp}')
                    resp.raise_for_status()
                except HTTPError as exc:
                    logger.exception('Exception occurred!')
                    latest_exc = exc
                    # See if a handler has been provided for the status code and
                    # call if provided, passing in the function and args.
                    contingency_func = self.http_code_handlers.get(
                        resp.status_code)
                    if contingency_func:
                        # Contingency function can return kwargs to replace
                        # the existing ones.
                        contingency_kwargs = contingency_func(
                            resp, req_func, *args, **kwargs)
                        kwargs = contingency_kwargs or kwargs
                    wait += self.backoff
                else:
                    return resp
            # To handle case where error is not resolved
            logger.error(
                'HTTP error was unable to be resolved through retries.')
            raise latest_exc
        return wrap

    
@singledispatch
def clean_data(data):
    """
    Dispatch function meant to clean arbitrarily nested and messy data
    structures.  Will clean data in a dictionary, list, or string format.
    Other data types are ignored.

    :param data: A data structure that will typically come from a JSON object,
     and thus will most often be a dictionary or list.
    """
    pass


@clean_data.register(numbers.Real)
def return_num(n):
    return n


@clean_data.register(str)
def clean_str(s):
    rx = re.compile('[\r\n|]')
    return rx.sub(' ', s)


@clean_data.register(dict)
def clean_dict(d):
    for key, value in d.items():
        if isinstance(value, str):
            d[key] = clean_data(value)
        else:
            clean_data(value)


@clean_data.register(list)
def clean_list(l):
    for index, item in enumerate(l):
        if isinstance(item, str):
            l[index] = clean_data(item)
        else:
            clean_data(item)


if __name__ == '__main__':
    pass
