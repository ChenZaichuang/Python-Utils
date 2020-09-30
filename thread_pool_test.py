import datetime

import unittest2

from python_utils.thread_pool import ThreadPool
import gevent


class ThreadPoolTest(unittest2.TestCase):

    def func_with_args_and_kwargs(self, param, *args, **kwargs):
        return ','.join([str(param), str(args), str(kwargs)])

    def gevent_func_with_sleep(self, param, sleep_second=0.1):
        gevent.sleep(sleep_second)
        return param

    def gevent_func_with_sleep_and_exception(self, sleep_second=0.1):
        gevent.sleep(sleep_second)
        raise RuntimeError("Not Killed")

    def test_gevent_thread_pool_should_get_results_order_by_index(self):
        pool = ThreadPool(total_thread_number=2)
        pool.apply_async(self.func_with_args_and_kwargs, args=(1, 2, 3), kwargs=dict(a=4, b=5))
        pool.apply_async(self.func_with_args_and_kwargs, args=(11, 22, 33), kwargs=dict(a=44, b=55))
        res = pool.get_results_order_by_index()
        self.assertEqual("1,(2, 3),{'a': 4, 'b': 5}", res[0])
        self.assertEqual("11,(22, 33),{'a': 44, 'b': 55}", res[1])

    def test_gevent_thread_pool_should_get_results_order_by_time(self):
        pool = ThreadPool(total_thread_number=2)
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.2))
        pool.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.1))
        expected_result_by_time = [2, 1]
        for index, res in enumerate(pool.get_results_order_by_time()):
            self.assertLess(index, 2, f"Wrong index: {index}")
            self.assertEqual(expected_result_by_time[index], res)

    def test_gevent_thread_pool_should_block_new_async_job_when_pool_full(self):
        pool = ThreadPool(total_thread_number=1)
        start_time = datetime.datetime.now()
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.1))
        time_of_job1_finish = datetime.datetime.now()
        pool.get_results_order_by_index()
        time_of_job2_finish = datetime.datetime.now()
        self.assertAlmostEqual((time_of_job1_finish - start_time).microseconds / 1000000, 0.1, 1)
        self.assertAlmostEqual((time_of_job2_finish - start_time).microseconds / 1000000, 0.2, 1)

    def test_gevent_thread_pool_should_share_pool_with_same_capacity(self):
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

    def test_gevent_thread_pool_should_share_pool_with_smaller_capacity(self):
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

    def test_gevent_thread_pool_should_share_pool_with_larger_capacity(self):
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

    def test_gevent_thread_pool_should_stop_all_threads(self):
        pool = ThreadPool(total_thread_number=2, exit_for_any_exception=True)
        start_time = datetime.datetime.now()
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.2))
        pool.stop_all()
        pool.wait_all_threads(raise_exception=True)
        end_time = datetime.datetime.now()
        self.assertAlmostEqual((end_time - start_time).microseconds / 1000000, 0, 1)
        gevent.sleep(0.3)

    def test_gevent_thread_pool_should_stop_nth_threads(self):
        pool = ThreadPool(total_thread_number=2, exit_for_any_exception=True)
        start_time = datetime.datetime.now()
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.2))
        pool.stop_nth_thread(1)
        pool.wait_all_threads(raise_exception=True)
        end_time = datetime.datetime.now()
        self.assertAlmostEqual((end_time - start_time).microseconds / 1000000, 0.1, 1)
        gevent.sleep(0.2)

    def test_gevent_thread_pool_should_reuse_pool_after_stop_threads(self):
        pool = ThreadPool(total_thread_number=2)
        start_time = datetime.datetime.now()
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.2))
        pool.stop_nth_thread(0)
        pool.apply_async(self.gevent_func_with_sleep, args=(3,), kwargs=dict(sleep_second=0.3))
        res = pool.get_results_order_by_index()
        end_time = datetime.datetime.now()
        self.assertAlmostEqual((end_time - start_time).microseconds / 1000000, 0.3, 1)
        self.assertEqual(2, res[1])
        self.assertEqual(3, res[2])

        start_time = datetime.datetime.now()
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.2))
        pool.stop_all()
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(3,), kwargs=dict(sleep_second=0.1))
        res = pool.get_results_order_by_index()
        end_time = datetime.datetime.now()
        self.assertAlmostEqual((end_time - start_time).microseconds / 1000000, 0.2, 1)
        self.assertEqual(1, res[2])
        self.assertEqual(2, res[3])
        self.assertEqual(3, res[4])

    def test_gevent_thread_pool_should_get_result_by_index_after_kill_one_and_get_one_result(self):
        pool = ThreadPool(total_thread_number=4, exit_for_any_exception=True)
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.2))
        pool.apply_async(self.gevent_func_with_sleep, args=(3,), kwargs=dict(sleep_second=0.3))
        pool.stop_nth_thread(0)
        quickest_result = pool.get_one_result(raise_exception=True)
        self.assertEqual(1, quickest_result)
        results = pool.get_results_order_by_index(raise_exception=True)
        self.assertEqual(2, results[2])
        self.assertEqual(3, results[3])

    def test_gevent_thread_pool_should_get_result_by_time_after_kill_one_and_get_one_result(self):
        pool = ThreadPool(total_thread_number=4, exit_for_any_exception=True)
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(1,), kwargs=dict(sleep_second=0.1))
        pool.apply_async(self.gevent_func_with_sleep, args=(2,), kwargs=dict(sleep_second=0.3))
        pool.apply_async(self.gevent_func_with_sleep, args=(3,), kwargs=dict(sleep_second=0.2))
        pool.stop_nth_thread(0)
        quickest_result = pool.get_one_result(raise_exception=True)
        self.assertEqual(1, quickest_result)
        expected_result_by_time = [3, 2]
        for index, res in enumerate(pool.get_results_order_by_time(raise_exception=True)):
            self.assertLess(index, 2, f"Wrong index: {index}")
            self.assertEqual(expected_result_by_time[index], res)

    def test_gevent_thread_pool_should_raise_exception_when_any_exception_happen(self):
        pool = ThreadPool(total_thread_number=1, exit_for_any_exception=False)
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))
        with self.assertRaisesRegex(RuntimeError, "^Not Killed$"):
            pool.get_results_order_by_index(raise_exception=True)
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))
        with self.assertRaisesRegex(RuntimeError, "^Not Killed$"):
            for _ in pool.get_results_order_by_time(raise_exception=True):
                pass

    def test_gevent_thread_pool_should_get_exception_when_any_exception_happen(self):
        pool = ThreadPool(total_thread_number=1, exit_for_any_exception=False)
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))
        res = pool.get_results_order_by_index(raise_exception=False)
        self.assertTrue(type(res[0]) == RuntimeError)
        self.assertEqual("Not Killed", str(res[0]))
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))
        for res in pool.get_results_order_by_time(raise_exception=False):
            self.assertTrue(type(res) == RuntimeError)
            self.assertEqual("Not Killed", str(res))

    def test_gevent_thread_pool_should_exit_program_when_any_exception_happen(self):
        pool = ThreadPool(total_thread_number=1, exit_for_any_exception=True)
        pool.apply_async(self.gevent_func_with_sleep_and_exception, kwargs=dict(sleep_second=0.1))
        with self.assertRaises(SystemExit):
            pool.get_results_order_by_index(raise_exception=True)
