import datetime
import os, sys
from unittest import mock

import unittest2

from gevent_thread_pool import GeventThreadPool as ThreadPool
from gevent import sleep
from greenlet import GreenletExit as ExitException

import logging
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

# from native_thread_pool import NativeThreadPool as ThreadPool
# from time import sleep
# ExitException = SystemExit

class ThreadPoolTest(unittest2.TestCase):

    def func_with_args_and_kwargs(self, param, *args, **kwargs):
        return ','.join([str(param), str(args), str(kwargs)])

    def gevent_func_with_sleep(self, param, sleep_second=0.1):
        sleep(sleep_second)
        logging.info(f'param: {param}')
        return param

    def gevent_func_with_sleep_and_exception(self, sleep_second=0.1):
        sleep(sleep_second)
        raise RuntimeError("Not Killed")

    def gevent_func_with_sleep_and_exception_and_update_list(self, a_list, sleep_second=0.1):
        assert len(a_list) == 1
        sleep(sleep_second)
        a_list[0] += 1
        logging.info(f'add !!!!!!!')
        raise RuntimeError("Not Killed")

    def test_thread_pool_should_get_results_order_by_index(self):
        pool = ThreadPool(total_thread_number=2)
        pool.apply_async(self.func_with_args_and_kwargs, args=(1, 2, 3), kwargs=dict(a=4, b=5))
        pool.apply_async(self.func_with_args_and_kwargs, args=(11, 22, 33), kwargs=dict(a=44, b=55))
        res = pool.get_results_order_by_index()
        self.assertEqual("1,(2, 3),{'a': 4, 'b': 5}", res[0])
        self.assertEqual("11,(22, 33),{'a': 44, 'b': 55}", res[1])

    def test_thread_pool_should_get_results_order_by_time(self):
        pool = ThreadPool(total_thread_number=2)
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.2))
        pool.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.1))
        expected_result_by_time = [2, 1]
        for index, res in enumerate(pool.get_results_order_by_time()):
            self.assertLess(index, 2, f"Wrong index: {index}")
            self.assertEqual(expected_result_by_time[index], res)

    def test_thread_pool_should_block_new_async_job_when_pool_full(self):
        pool = ThreadPool(total_thread_number=1)
        start_time = datetime.datetime.now()
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.1))
        time_of_job1_finish = datetime.datetime.now()
        pool.get_results_order_by_index()
        time_of_job2_finish = datetime.datetime.now()
        self.assertAlmostEqual((time_of_job1_finish - start_time).microseconds / 1000000, 0.1, 1)
        self.assertAlmostEqual((time_of_job2_finish - start_time).microseconds / 1000000, 0.2, 1)

    def test_thread_pool_should_share_pool_with_same_capacity(self):
        pool = ThreadPool(total_thread_number=1)
        start_time = datetime.datetime.now()
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool_1 = pool.new_shared_pool()
        pool_1.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.1))
        job1_inserted_time = datetime.datetime.now()
        pool_1.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.1))
        job2_inserted_time = datetime.datetime.now()
        pool_1.wait_all_threads()
        job_finished_time = datetime.datetime.now()
        self.assertAlmostEqual((job1_inserted_time - start_time).microseconds / 1000000, 0.2, 1)
        self.assertAlmostEqual((job2_inserted_time - start_time).microseconds / 1000000, 0.3, 1)
        self.assertAlmostEqual((job_finished_time - start_time).microseconds / 1000000, 0.4, 1)

    def test_thread_pool_should_share_pool_with_smaller_capacity(self):
        pool = ThreadPool(total_thread_number=2)
        start_time = datetime.datetime.now()
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool_1 = pool.new_shared_pool(max_thread=1)
        pool_1.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.1))
        time_of_job1_finish = datetime.datetime.now()
        pool_1.wait_all_threads()
        time_of_job2_finish = datetime.datetime.now()
        self.assertAlmostEqual((time_of_job1_finish - start_time).microseconds / 1000000, 0.1, 1)
        self.assertAlmostEqual((time_of_job2_finish - start_time).microseconds / 1000000, 0.2, 1)

    def test_thread_pool_should_share_pool_with_larger_capacity(self):
        pool = ThreadPool(total_thread_number=1)
        start_time = datetime.datetime.now()
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool_1 = pool.new_shared_pool(2)
        pool_1.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.1))
        job1_inserted_time = datetime.datetime.now()
        pool_1.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.1))
        job2_inserted_time = datetime.datetime.now()
        pool_1.wait_all_threads()
        job_finished_time = datetime.datetime.now()
        self.assertAlmostEqual((job1_inserted_time - start_time).microseconds / 1000000, 0.2, 1)
        self.assertAlmostEqual((job2_inserted_time - start_time).microseconds / 1000000, 0.3, 1)
        self.assertAlmostEqual((job_finished_time - start_time).microseconds / 1000000, 0.4, 1)

    def test_thread_pool_should_reuse_pool_after_stop_threads(self):
        pool = ThreadPool(total_thread_number=2)

        a_list = [1]
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0.1))
        pool.stop_all()
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0.1))
        pool.get_results_order_by_index()
        self.assertEqual([4], a_list)

    def test_thread_pool_should_get_result_by_index_after_kill_one_and_get_one_result(self):
        pool = ThreadPool(total_thread_number=4, exit_for_any_exception=True)
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.2))
        pool.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.3))
        pool.apply_async(self.gevent_func_with_sleep, args=(3,), kwargs=dict(sleep_second=0.4))
        pool.stop_nth_thread(0)
        quickest_result = pool.get_one_result(raise_exception=True)
        self.assertEqual(1, quickest_result)
        results = pool.get_results_order_by_index(raise_exception=True)
        self.assertEqual(2, results[2])
        self.assertEqual(3, results[3])

    def test_thread_pool_should_get_result_by_time_after_kill_one_and_get_one_result(self):
        pool = ThreadPool(total_thread_number=4, exit_for_any_exception=True)
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.2))
        pool.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.4))
        pool.apply_async(self.gevent_func_with_sleep, args=(3,), kwargs=dict(sleep_second=0.3))
        pool.stop_nth_thread(0)
        quickest_result = pool.get_one_result(raise_exception=True)
        self.assertEqual(1, quickest_result)
        expected_result_by_time = [3, 2]
        actual_result_by_time = []
        for index, res in enumerate(pool.get_results_order_by_time(raise_exception=True)):
            actual_result_by_time.append(res)
        logging.info(expected_result_by_time)
        logging.info(actual_result_by_time)
        self.assertEqual(expected_result_by_time, actual_result_by_time)

    def test_thread_pool_should_raise_exception_when_any_exception_happen(self):
        pool = ThreadPool(total_thread_number=2, exit_for_any_exception=False)

        a_list = [1]
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0))
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0.2))
        with self.assertRaisesRegex(RuntimeError, "^Not Killed$"):
            pool.get_results_order_by_index(raise_exception=True, stop_all_for_exception=False)
        sleep(0.3)
        assert a_list[0] == 3

        a_list = [1]
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0))
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0.2))
        with self.assertRaisesRegex(RuntimeError, "^Not Killed$"):
            for _ in pool.get_results_order_by_time(raise_exception=True, stop_all_for_exception=False):
                pass
        sleep(0.3)
        assert a_list[0] == 3

        a_list = [1]
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0))
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0.2))
        with self.assertRaisesRegex(RuntimeError, "^Not Killed$"):
            pool.get_one_result(raise_exception=True, stop_all_for_exception=False)
        sleep(0.3)
        assert a_list[0] == 3
        pool.refresh()

        a_list = [1]
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0))
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0.3))
        with self.assertRaisesRegex(RuntimeError, "^Not Killed$"):
            pool.get_results_order_by_index(raise_exception=True, stop_all_for_exception=True)
        sleep(0.4)
        assert a_list[0] == 2

        a_list = [1]
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0))
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0.3))
        with self.assertRaisesRegex(RuntimeError, "^Not Killed$"):
            for _ in pool.get_results_order_by_time(raise_exception=True, stop_all_for_exception=True):
                pass
        sleep(0.4)
        assert a_list[0] == 2

        logging.info('\n\nanother test\n\n')

        a_list = [1]
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0))
        pool.apply_async(self.gevent_func_with_sleep_and_exception_and_update_list, args=(a_list,), kwargs=dict(sleep_second=0.3))
        with self.assertRaisesRegex(RuntimeError, "^Not Killed$"):
            pool.get_one_result(raise_exception=True, stop_all_for_exception=True)
        sleep(0.4)
        assert a_list[0] == 2

    def test_thread_pool_should_get_exception_when_any_exception_happen(self):
        pool = ThreadPool(total_thread_number=1, exit_for_any_exception=False)
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))
        res = pool.get_results_order_by_index(raise_exception=False)
        self.assertTrue(type(res[0]) == RuntimeError)
        self.assertEqual("Not Killed", str(res[0]))
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))
        for res in pool.get_results_order_by_time(raise_exception=False):
            self.assertTrue(type(res) == RuntimeError)
            self.assertEqual("Not Killed", str(res))

    def test_thread_pool_should_exit_program_when_any_exception_happen(self):

        os._exit = mock.Mock()
        sys.exit = os._exit

        pool = ThreadPool(total_thread_number=1, exit_for_any_exception=True)
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))

        try:
            pool.get_results_order_by_index(raise_exception=True)
        except:
            pass
        os._exit.assert_called_once()
