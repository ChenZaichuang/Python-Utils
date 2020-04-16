import logging
import sys


class Logger:

    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stdout_stream_handler = logging.StreamHandler(sys.stdout)
    stdout_stream_handler.setFormatter(log_formatter)
    stderr_stream_handler = logging.StreamHandler(sys.stderr)
    stderr_stream_handler.setFormatter(log_formatter)

    stdout_logger = logging.getLogger("stdout_logger")
    stdout_logger.setLevel(logging.INFO)
    stdout_logger.addHandler(stdout_stream_handler)

    stderr_logger = logging.getLogger("stderr_logger")
    stderr_logger.setLevel(logging.ERROR)
    stderr_logger.addHandler(stderr_stream_handler)

    @staticmethod
    def info(msg):
        Logger.stdout_logger.info(msg)

    @staticmethod
    def error(msg):
        Logger.stderr_logger.error(msg)


class CustomLogger:
    console_logger_index = 0
    file_logger_index = 0

    def __init__(self, console_logger_config=None, file_logger_config=None):
        self.console_level = None
        self.file_level = None

        if console_logger_config:
            self.console_level = console_logger_config.get('level', logging.INFO)
            self.stdout_logger = logging.getLogger(f"stdout_logger_{CustomLogger.console_logger_index}")
            self.stdout_logger.setLevel(self.console_level)
            self.stdout_logger.addHandler(Logger.stdout_stream_handler)

            self.stderr_logger = logging.getLogger(f"stderr_logger_{CustomLogger.console_logger_index}")
            self.stderr_logger.setLevel(logging.ERROR)
            self.stderr_logger.addHandler(Logger.stderr_stream_handler)

            CustomLogger.console_logger_index += 1

        if file_logger_config:
            self.file_level = file_logger_config.get('level', logging.INFO)
            self.file_logger = logging.getLogger(f"file_logger_{CustomLogger.file_logger_index}")
            self.file_logger.setLevel(self.file_level)
            file_handler = logging.FileHandler(file_logger_config['filename'], mode=file_logger_config.get('mode', 'w'))
            file_handler.setFormatter(Logger.log_formatter)
            self.file_logger.addHandler(file_handler)
            CustomLogger.file_logger_index += 1

    def debug(self, msg):
        if self.console_level and self.console_level <= logging.DEBUG:
            self.stdout_logger.debug(msg)
        if self.file_level and self.file_level <= logging.INFO:
            self.file_logger.debug(msg)

    def info(self, msg):
        if self.console_level and self.console_level <= logging.INFO:
            self.stdout_logger.info(msg)
        if self.file_level and self.file_level <= logging.INFO:
            self.file_logger.info(msg)

    def error(self, msg):
        if self.console_level and self.console_level <= logging.ERROR:
            self.stderr_logger.error(msg)
        if self.file_level and self.file_level <= logging.INFO:
            self.file_logger.error(msg)


if __name__ == '_main__':

    Logger.info('info')
    Logger.error('error')

    logger = CustomLogger(console_logger_config={'level': logging.INFO}, file_logger_config={'filename': 'test.log', 'mode': 'w', 'level': logging.DEBUG})
    logger.debug('debug 1')
    logger.info('info 1')
    logger.error('error 1')