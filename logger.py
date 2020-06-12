import logging
import os
import sys


class CustomLogger:
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stdout_stream_handler = logging.StreamHandler(sys.stdout)
    stdout_stream_handler.setFormatter(log_formatter)
    stderr_stream_handler = logging.StreamHandler(sys.stderr)
    stderr_stream_handler.setFormatter(log_formatter)

    console_logger_index = 0
    file_logger_index = 0

    def __init__(self, level=logging.DEBUG, to_console=True, to_file_name='', with_requests_logger=False):

        level = int(os.environ.get('logging_level', level))
        to_console = str(os.environ.get('logging_to_console', to_console))
        to_file_name = os.environ.get('logging_to_file_name', to_file_name)
        with_requests_logger = str(os.environ.get('logging_with_requests_logger', with_requests_logger))

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
            file_handler = logging.FileHandler(to_file_name, mode='w')
            file_handler.setFormatter(self.log_formatter)
            self.stdout_logger.addHandler(file_handler)
            self.stderr_logger.addHandler(file_handler)

    @classmethod
    def set_global_config(cls, level=logging.DEBUG, to_console=True, to_file_name='', with_requests_logger=False):
        os.environ['logging_level'] = str(level)
        os.environ['logging_to_console'] = str(to_console)
        os.environ['logging_to_file_name'] = to_file_name
        os.environ['logging_with_requests_logger'] = str(with_requests_logger)

    def debug(self, msg):
        self.stdout_logger.debug(msg)

    def info(self, msg):
        self.stdout_logger.info(msg)

    def error(self, msg):
        self.stderr_logger.error(msg)


if __name__ == '__main__':

    CustomLogger.set_global_config(level=logging.INFO, with_requests_logger=True)

    logger = CustomLogger(level=logging.DEBUG, to_file_name='test.log', with_requests_logger=True)
    try:
        import requests
        requests.get('http://www.baidu.com')
    except:
        pass
    logger.debug('debug')
    logger.info('info')
    logger.error('error')