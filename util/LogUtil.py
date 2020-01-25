# coding:utf-8
import logging,logging.handlers
import os


class LogUtil(object):
    def __init__(self, log_file_name):
        self.logger = logging.getLogger(log_file_name)
        self.logger.setLevel(logging.DEBUG)
        log_path = "/".join(os.path.abspath(os.path.dirname(__file__)).split("/")[:-1]) + "/log"
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        log_file_path = "%s/%s.log" % (log_path, log_file_name)

        # 文件handler
        fh = logging.handlers.TimedRotatingFileHandler(log_file_path, 'D', 1, 30)
        fh.setLevel(logging.DEBUG)
        # 控制台handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # 定义handler的输出格式
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def debug(self, msg):
        self.logger.debug(msg)


if __name__ == "__main__":
    logUtil = LogUtil("test_log_name")
    logUtil.info("test")
