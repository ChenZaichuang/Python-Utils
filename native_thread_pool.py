import ctypes
import logging
# from multiprocessing.dummy import Pool, BoundedSemaphore, Process, DummyProcess
import os
from queue import Queue
import sys
import traceback
import threading
from threading import BoundedSemaphore



class ThreadWithTrace(threading.Thread):
    def __init__(self, *args, **keywords):
        threading.Thread.__init__(self, *args, **keywords)
        self.killed = False

    def start(self):
        self.__run_backup = self.run
        self.run = self.__run
        threading.Thread.start(self)

    def __run(self):
        sys.settrace(self.globaltrace) #精髓在这里，但我还没有理解为什么要这么做。。。
        self.__run_backup()
        self.run = self.__run_backup

    def globaltrace(self, frame, event, arg):
        if event == 'call':
            return self.localtrace
        else:
            return None

    def localtrace(self, frame, event, arg):
        if self.killed:
            if event == 'line':
                raise SystemExit()
        return self.localtrace

    def kill(self):
        self.killed = True


# class ThreadWithTrace(threading.Thread):
#
#     def __init__(self, target, args=None, kwargs=None):
#         super(ThreadWithTrace, self).__init__()
#         self.target = target
#         self.args = args if args is not None else ()
#         self.kwargs = kwargs if kwargs is not None else dict()
#         self.stopped = False
#
#     def run(self):
#         subthread = threading.Thread(target=self.target, args=self.args, kwargs=self.kwargs)
#         subthread.setDaemon(True)
#         subthread.start()
#
#         while not self.stopped and subthread.is_alive():
#             subthread.join(0.01)
#
#     def kill(self):
#         self.stopped = True
#
#     def isStopped(self):
#         return self.stopped


class NativeThreadPool:

    # def __init__(self, **kwargs):
    #     assert 'processes' in kwargs or 'thread_pool' in kwargs
    #     self.thread_pool = Pool(kwargs['processes']) if 'processes' in kwargs else kwargs['thread_pool']
    #     self.exit_for_any_exception = kwargs.get("exit_for_any_exception", False)
    #     self._thread_res_queue = Queue()
    #     self.thread_number = 0
    #     self.valid_for_new_thread = True

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
        main_semaphore = self.main_semaphore
        sub_semaphore = self.sub_semaphore
        thread_res_queue = self._thread_res_queue
        completed_threads = self.completed_threads
        killed_threads = self.killed_threads
        try:
            res = func(*args, **kwargs)
        except Exception as e:
            res = e
            err_msg = traceback.format_exc()
            if self.log_exception:
                logging.error(f"Thread {thread_number} failed when execute {func.__name__}, error msg: \n{err_msg}")
            if self.exit_for_any_exception:
                # logging.info('666666')
                # raise SystemExit()
                # sys.exit(-1)
                os._exit(-1)

            success = False
        finally:
            logging.info(f'main_semaphore.release(): {thread_number}')
            main_semaphore.release()
            sub_semaphore.release()
            completed_threads.add(thread_number)
            if thread_number not in killed_threads:
                thread_res_queue.put((thread_number, success, res))

    def apply_async(self, func, args=None, kwargs=None):
        assert self.valid_for_new_thread
        logging.info(f'main_semaphore.acquire(): {len(self.thread_list)}')
        self.main_semaphore.acquire()
        self.sub_semaphore.acquire()
        if args is None:
            args = tuple()
        if kwargs is None:
            kwargs = dict()
        # thread = Process(target=self.start_thread, args=(len(self.thread_list), func, args, kwargs))
        # thread = threading.Thread(target=self.start_thread, args=(len(self.thread_list), func, args, kwargs))
        thread = ThreadWithTrace(target=self.start_thread, args=(len(self.thread_list), func, args, kwargs))
        self.thread_list.append(thread)
        thread.start()
        # thread = gevent.spawn(self.start_thread, len(self.thread_list), func, args, kwargs)

        return thread

    def new_shared_pool(self, exit_for_any_exception=False, max_thread=0):
        return NativeThreadPool(semaphore=self.main_semaphore, exit_for_any_exception=exit_for_any_exception,
                                max_thread=max_thread if max_thread > 0 else self.max_thread)

    def get_results_order_by_index(self, raise_exception=False, with_status=False, stop_all_for_exception=False):
        self.valid_for_new_thread = False
        threads_result = [''] * len(self.thread_list) if with_status else [('', '')] * len(self.thread_list)
        logging.info(f"thread_list: {self.thread_list}")
        logging.info(f"killed_threads: {self.killed_threads}")
        for index in range(len(self.thread_list) - len(self.killed_threads)):
            thread_number, success, res = self._thread_res_queue.get()

            logging.info(f"thread_number: {thread_number}")
            logging.info(f"success: {success}")
            logging.info(f"res: {res}")
            if thread_number in self.killed_threads:
                continue

            if success or not raise_exception:
                threads_result[thread_number] = (success, res) if with_status else res
            else:
                if stop_all_for_exception:
                    logging.info('stop all')
                    self.stop_all()
                self.refresh()
                raise res
        self.refresh()
        return threads_result

    def get_results_order_by_time(self, raise_exception=False, with_status=False, stop_all_for_exception=False):
        self.valid_for_new_thread = False
        for index in range(len(self.thread_list) - len(self.killed_threads)):
            thread_number, success, res = self._thread_res_queue.get()
            # if thread_number in self.killed_threads:
            #     continue
            if success or not raise_exception:
                if index == len(self.thread_list) - 1:
                    self.refresh()
                yield (success, res) if with_status else res
            else:
                if stop_all_for_exception:
                    self.stop_all()
                self.refresh()
                raise res

    def get_one_result(self, raise_exception=False, with_status=False, stop_all_for_exception=False):
        while True:
            thread_number, success, res = self._thread_res_queue.get()
            if thread_number in self.killed_threads:
                continue
            self.killed_threads.add(thread_number)
            if not success and raise_exception:
                if stop_all_for_exception:
                    self.stop_all()
                raise res
            return (success, res) if with_status else res

    def wait_all_threads(self, raise_exception=False):
        for thread in self.thread_list:
            thread.join()
        # gevent.joinall(self.thread_list)
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

            logging.info(f'stop_nth_thread {n}')

            self.completed_threads.add(n)
            self.killed_threads.add(n)
            # self.thread_list[n]._stop()
            self.thread_list[n].kill()
            # self.thread_list[n].join()
            # # self.thread_list[n]._stop.set()
            # ctypes.pythonapi.PyThreadState_SetAsyncExc(self.thread_list[n],
            #                                            ctypes.py_object(SystemExit))
            # if n not in self.completed_threads:

            #     logging.info(f'@@@@@@@ main_semaphore.release(): {self.main_semaphore._value}')
            #     self.main_semaphore.release()
            #     self.sub_semaphore.release()

    def refresh(self):
        logging.info('\n refresh\n')
        self.thread_list = []
        self.valid_for_new_thread = True
        self.completed_threads = set()
        self.killed_threads = set()
        self._thread_res_queue = Queue()

        for thread in self.thread_list:
            thread.join()

    @classmethod
    def new_thread(cls):
        pass


    # def start_thread(self, thread_number, func, args=(), kwds=None):
    #     if kwds is None:
    #         kwds = {}
    #     success = True
    #     res = None
    #     try:
    #         res = func(*args, **kwds)
    #     except Exception:
    #         res = traceback.format_exc()
    #         logging.info(f"Thread {thread_number} failed when execute {func.__name__}, error msg: \n{res}")
    #         if self.exit_for_any_exception:
    #             os._exit(1)
    #         success = False
    #     finally:
    #         self._thread_res_queue.put((thread_number, success, res))
    #
    # def apply_async(self, func, args=(), kwds=None):
    #     assert self.valid_for_new_thread
    #     if kwds is None:
    #         kwds = {}
    #     self.thread_pool.apply_async(self.start_thread, (self.thread_number, func, args, kwds))
    #     self.thread_number += 1
    #     return self
    #
    # def get_threads_result(self, raise_exception=False, with_status=True):
    #     self.valid_for_new_thread = False
    #     threads_result = [0]*self.thread_number if with_status else [(0, 0)]*self.thread_number
    #     while self.thread_number:
    #         thread_number, success, res = self._thread_res_queue.get()
    #         if success or not raise_exception:
    #             threads_result[thread_number] = (success, res) if with_status else res
    #         else:
    #             raise Exception(f"op=execute thread {thread_number} | status=ERROR | desc={res}")
    #         self.thread_number -= 1
    #     return threads_result
    #
    # def new_pool_status(self, exit_for_any_exception=False):
    #     return NativeThreadPool(thread_pool=self.thread_pool, exit_for_any_exception=exit_for_any_exception)
    #
    # def get_result_queue(self):
    #     return self._thread_res_queue

