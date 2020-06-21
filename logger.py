import logging
from logging.handlers import TimedRotatingFileHandler
import sys

from prettytable import PrettyTable, ALL


class CustomLogger:
    global_config_set = False
    global_stdout_logger = None
    global_stderr_logger = None

    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stdout_stream_handler = logging.StreamHandler(sys.stdout)
    stdout_stream_handler.setFormatter(log_formatter)
    stderr_stream_handler = logging.StreamHandler(sys.stderr)
    stderr_stream_handler.setFormatter(log_formatter)

    console_logger_index = 0
    file_logger_index = 0

    def __init__(self, level=logging.DEBUG, to_console=True, to_file_name='', with_requests_logger=False, time_rotating=None, use_global_config=True):

        if use_global_config and CustomLogger.global_config_set:
            self.stdout_logger = CustomLogger.global_stdout_logger
            self.stderr_logger = CustomLogger.global_stderr_logger
        else:
            if time_rotating is not None:
                assert type(time_rotating) is dict and "when" in time_rotating and "interval" in time_rotating

            if with_requests_logger:
                self.stdout_logger = logging.getLogger("urllib3")
            else:
                self.stdout_logger = logging.getLogger(f"stdout_logger_{CustomLogger.console_logger_index}")

            self.stdout_logger.propagate = False
            self.stdout_logger.setLevel(level)

            self.stderr_logger = logging.getLogger(f"stderr_logger_{CustomLogger.console_logger_index}")
            self.stderr_logger.propagate = False
            self.stderr_logger.setLevel(logging.ERROR)

            if to_console:
                self.stdout_logger.addHandler(self.stdout_stream_handler)
                self.stderr_logger.addHandler(self.stderr_stream_handler)

            if to_file_name:
                if time_rotating is None:
                    file_handler = logging.FileHandler(to_file_name, mode='w')
                else:
                    file_handler = TimedRotatingFileHandler(filename=to_file_name, when=time_rotating['when'], interval=time_rotating['interval'])
                file_handler.setFormatter(self.log_formatter)
                self.stdout_logger.addHandler(file_handler)
                self.stderr_logger.addHandler(file_handler)

    @classmethod
    def set_global_config(cls, level=logging.DEBUG, to_console=True, to_file_name='', with_requests_logger=False, time_rotating=None):
        logger = CustomLogger(level=level, to_console=to_console, to_file_name=to_file_name, with_requests_logger=with_requests_logger, time_rotating=time_rotating)
        CustomLogger.global_stdout_logger = logger.stdout_logger
        CustomLogger.global_stderr_logger = logger.stderr_logger
        CustomLogger.global_config_set = True
        Logger.logger = logger

    def debug(self, msg):
        self.stdout_logger.debug(msg)

    def info(self, msg):
        self.stdout_logger.info(msg)

    def error(self, msg):
        self.stderr_logger.error(msg)


class Logger:
    logger = CustomLogger()

    @classmethod
    def debug(cls, msg):
        Logger.logger.debug(msg)

    @classmethod
    def info(cls, msg):
        Logger.logger.info(msg)

    @classmethod
    def error(cls, msg):
        Logger.logger.error(msg)

    @classmethod
    def rows_to_table_string(cls, rows):
        if len(rows) == 0:
            return ''
        table = PrettyTable()
        table.field_names = rows[0]
        for row in rows[1:]:
            table.add_row(row)
        return table.get_string(hrules=ALL)


if __name__ == '__main__':

    CustomLogger.set_global_config(level=logging.INFO, to_file_name='test.log', with_requests_logger=True)

    logger = CustomLogger(level=logging.DEBUG, to_file_name='test.log', with_requests_logger=True)
    try:
        import requests
        requests.get('http://www.baidu.com')
    except:
        pass
    logger.debug('debug')
    logger.info('info')
    logger.error('error')

    Logger.debug('debug1')
    Logger.info('info1')
    Logger.error('error1')
