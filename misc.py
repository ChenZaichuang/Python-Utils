import os
import pickle
from datetime import timedelta, datetime
import functools
import traceback

from gevent import Timeout, sleep
from gevent.timeout import Timeout as TimeoutException


def get_variable_from_local(variable_name_with_path, default_value=None, function_value=None, args=None, kwds=None, keep_time=None, local_expire_datetime=None, force_refresh=False):
    assert default_value or function_value
    if "pkl_file_path" in os.environ:
        variable_name_with_path = f'{os.environ["pkl_file_path"]}/{variable_name_with_path}'

    if (not force_refresh) and os.path.exists(f'{variable_name_with_path}.pkl'):
        with open(f'{variable_name_with_path}.pkl', 'rb') as f:
            info_map = pickle.load(f)
        if keep_time or local_expire_datetime:
            retire_datetime = info_map["created_date"] + timedelta(seconds=int(keep_time)) if keep_time else datetime.strptime(local_expire_datetime, '%Y-%m-%d %H:%M:%S')
            current_time = datetime.now()
            if current_time <= retire_datetime:
                return info_map["variable_value"]
        else:
            return info_map["variable_value"]
    if args is None:
        args = ()
    if kwds is None:
        kwds = {}
    variable_value = default_value if default_value else function_value(*args, **kwds)
    with open(f'{variable_name_with_path}.pkl', 'wb') as f:
        pickle.dump({"variable_value": variable_value, "created_date": datetime.now()}, f)
    return variable_value


def date_range(start_date, end_date, including_end_date=False):
    for n in range(int((end_date - start_date).days) + (1 if including_end_date else 0)):
        yield start_date + timedelta(n)

def list_to_string(a_list):
    return "('" + "','".join(set(a_list)) + "')"

def set_to_string(a_set):
    return "('" + "','".join(a_set) + "')"

def timeout_retry(original_function=None, timeout=30, retries=5, retry_interval=3):

    def _decorate(function):

        @functools.wraps(function)
        def wrapped_function(*args, **kwargs):
            for _ in range(retries):
                try:
                    with Timeout(timeout):
                        res = original_function(*args, **kwargs)
                        if res is not None:
                            return res
                except TimeoutException:
                    sleep(retry_interval)
                except Exception:
                    err_msg = traceback.format_exc()
                    print(err_msg)
                    raise RuntimeError(f"Unknow Exception: {err_msg}")
            raise RuntimeError(f"Query Get None after {retries} times")

        return wrapped_function

    if original_function:
        return _decorate(original_function)

    return _decorate
