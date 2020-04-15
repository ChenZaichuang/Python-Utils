from gevent import monkey
monkey.patch_all(thread=False)
from gevent._semaphore import BoundedSemaphore
from gevent.libev.corecext import traceback
from gevent.pool import Group
from gevent.pool import Pool
from gevent.queue import Queue

import logging
import os
import signal


class ThreadPool:

    def __init__(self, **kwargs):
        assert 'total_thread_number' in kwargs or ('thread_pool' in kwargs and 'max_thread' in kwargs)
        self.thread_pool = Pool(kwargs['total_thread_number']) if 'total_thread_number' in kwargs else kwargs['thread_pool']
        self.max_thread = kwargs['max_thread'] if 'max_thread' in kwargs else kwargs['processes']
        self.semaphore = BoundedSemaphore(self.max_thread)
        self.exit_for_any_exception = kwargs.get("exit_for_any_exception", False)
        self._thread_res_queue = Queue()
        self.thread_number = 0
        self.valid_for_new_thread = True
        self.thread_group = Group()
        self.log_exception = kwargs.get('log_exception', True)

    def start_thread(self, thread_number, func, args=None, kwds=None):
        if args is None:
            args = ()
        if kwds is None:
            kwds = {}
        success = True
        res = None
        try:
            res = func(*args, **kwds)
        except Exception:
            res = traceback.format_exc()
            if self.log_exception:
                logging.error(f"Thread {thread_number} failed when execute {func.__name__}, error msg: \n{res}")
            if self.exit_for_any_exception:
                os.kill(os.getpid(),signal.SIGTERM)
            success = False
        finally:
            self.semaphore.release()
            self._thread_res_queue.put((thread_number, success, res))

    def apply_async(self, func, args=None, kwds=None, callback=None):
        assert self.valid_for_new_thread
        if args is None:
            args = ()
        self.semaphore.acquire()
        if kwds is None:
            kwds = {}
        thread = self.thread_pool.apply_async(self.start_thread, args=(self.thread_number, func, args, kwds),
                                              callback=callback)
        self.thread_group.add(thread)
        self.thread_number += 1
        return thread

    def new_pool_status(self, exit_for_any_exception=False, max_thread=0):
        if max_thread > 0:
            return ThreadPool(thread_pool=self.thread_pool, exit_for_any_exception=exit_for_any_exception,
                              max_thread=max_thread)
        else:
            return ThreadPool(thread_pool=self.thread_pool, exit_for_any_exception=exit_for_any_exception,
                              max_thread=self.max_thread)

    def get_results_order_by_index(self, raise_exception=False, with_status=False):
        self.valid_for_new_thread = False
        threads_result = [0] * self.thread_number if with_status else [(0, 0)] * self.thread_number
        while self.thread_number:
            thread_number, success, res = self._thread_res_queue.get()
            if success or not raise_exception:
                threads_result[thread_number] = (success, res) if with_status else res
            else:
                raise Exception(f"op=execute thread {thread_number} | status=ERROR | desc={res}")
            self.thread_number -= 1
        return threads_result

    def get_results_order_by_time(self, raise_exception=False, with_status=False):
        self.valid_for_new_thread = False
        while self.thread_number:
            thread_number, success, res = self._thread_res_queue.get()
            self.thread_number -= 1
            if success or not raise_exception:
                yield (success, res) if with_status else res
            else:
                raise Exception(f"op=execute thread {thread_number} | status=ERROR | desc={res}")

    def wait_all_threads(self, raise_exception=False):
        while self.thread_number:
            thread_number, success, res = self._thread_res_queue.get()
            if not success and not raise_exception:
                raise Exception(f"op=execute thread {thread_number} | status=ERROR | desc={res}")
            self.thread_number -= 1

    def stop_all(self):
        self.thread_group.kill()
