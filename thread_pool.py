import logging
import sys
import traceback

import gevent
from gevent._semaphore import BoundedSemaphore
from gevent.queue import Queue


class ThreadPool:

    __slots__ = ('max_thread', 'main_semaphore', 'sub_semaphore', 'exit_for_any_exception',
                 '_thread_res_queue', 'valid_for_new_thread', 'log_exception', 'thread_list',
                 'completed_threads', 'killed_threads')

    def __init__(self, **kwargs):
        assert 'total_thread_number' in kwargs or ('semaphore' in kwargs and 'max_thread' in kwargs)
        if 'total_thread_number' in kwargs:
            self.max_thread = kwargs['total_thread_number']
            self.main_semaphore = BoundedSemaphore(self.max_thread)
            self.sub_semaphore = BoundedSemaphore(self.max_thread)
        else:
            self.max_thread = kwargs['max_thread']
            self.main_semaphore = kwargs['semaphore']
            self.sub_semaphore = BoundedSemaphore(self.max_thread)
        self.exit_for_any_exception = kwargs.get("exit_for_any_exception", False)
        self._thread_res_queue = Queue()
        self.valid_for_new_thread = True
        self.log_exception = kwargs.get('log_exception', True)
        self.thread_list = []
        self.completed_threads = set()
        self.killed_threads = set()

    def start_thread(self, thread_number, func, args, kwargs):
        success = True
        res = None
        try:
            res = func(*args, **kwargs)
        except Exception as e:
            res = e
            err_msg = traceback.format_exc()
            if self.log_exception:
                logging.error(f"Thread {thread_number} failed when execute {func.__name__}, error msg: \n{err_msg}")
            if self.exit_for_any_exception:
                sys.exit()
            success = False
        finally:
            self.main_semaphore.release()
            self.sub_semaphore.release()
            self.completed_threads.add(thread_number)
            self._thread_res_queue.put((thread_number, success, res))

    def apply_async(self, func, args=None, kwargs=None):
        assert self.valid_for_new_thread
        self.main_semaphore.acquire()
        self.sub_semaphore.acquire()
        if args is None:
            args = tuple()
        if kwargs is None:
            kwargs = dict()
        thread = gevent.spawn(self.start_thread, len(self.thread_list), func, args, kwargs)
        self.thread_list.append(thread)
        return thread

    def new_shared_pool(self, exit_for_any_exception=False, max_thread=0):
        return ThreadPool(semaphore=self.main_semaphore, exit_for_any_exception=exit_for_any_exception,
                          max_thread=max_thread if max_thread > 0 else self.max_thread)

    def get_results_order_by_index(self, raise_exception=False, with_status=False):
        self.valid_for_new_thread = False
        threads_result = [''] * len(self.thread_list) if with_status else [('', '')] * len(self.thread_list)
        for _ in range(len(self.thread_list) - len(self.killed_threads)):
            thread_number, success, res = self._thread_res_queue.get()
            if success or not raise_exception:
                threads_result[thread_number] = (success, res) if with_status else res
            else:
                self.refresh()
                raise res
        self.refresh()
        return threads_result

    def get_results_order_by_time(self, raise_exception=False, with_status=False):
        self.valid_for_new_thread = False
        for index in range(len(self.thread_list) - len(self.killed_threads)):
            thread_number, success, res = self._thread_res_queue.get()
            if success or not raise_exception:
                if index == len(self.thread_list) - 1:
                    self.refresh()
                yield (success, res) if with_status else res
            else:
                self.refresh()
                raise res

    def get_one_result(self, raise_exception=False, with_status=False):
        thread_number, success, res = self._thread_res_queue.get()
        self.killed_threads.add(thread_number)
        if not success and raise_exception:
            raise res
        return (success, res) if with_status else res

    def wait_all_threads(self, raise_exception=False):
        gevent.joinall(self.thread_list)
        for _ in range(len(self.thread_list) - len(self.killed_threads)):
            thread_number, success, res = self._thread_res_queue.get()
            if not success and not raise_exception:
                self.refresh()
                raise res
        self.refresh()

    def stop_all(self):
        for index in range(len(self.thread_list)):
            self.stop_nth_thread(index)

    def stop_nth_thread(self, n):
        if n not in self.completed_threads:
            self.thread_list[n].kill()
            if n not in self.completed_threads:
                self.completed_threads.add(n)
                self.killed_threads.add(n)
                self.main_semaphore.release()
                self.sub_semaphore.release()

    def refresh(self):
        self.thread_list = []
        self.valid_for_new_thread = True
        self.completed_threads.clear()
        self.killed_threads.clear()
