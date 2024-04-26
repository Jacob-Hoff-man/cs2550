import logging
import os
from enum import Enum

from Constants import *


class LogType(Enum):
    CATALOG_MANAGER = "Catalog Manager"
    DATA_MANAGER = "Data Manager"
    DBMS_SIMULATOR = "DBMS Simulator"
    TRANSACTION_MANAGER = "Transaction Manager"


class Logger:
    def __init__(self, log_type):
        self.logger = self.get_logger(log_type)

    def get_log_name(self, log_type: LogType):
        return log_type.value

    def get_log_file_name(self, log_type: LogType):
        match log_type:
            case LogType.CATALOG_MANAGER:
                base_file_path = LOG_CTLG_MGR_OUTPUT_FILE_PATH
            case LogType.DATA_MANAGER:
                base_file_path = LOG_DATA_MGR_OUTPUT_FILE_PATH
            case LogType.DBMS_SIMULATOR:
                base_file_path = LOG_DBMS_SIM_OUTPUT_FILE_PATH
            case LogType.TRANSACTION_MANAGER:
                base_file_path = LOG_TRXN_MGR_OUTPUT_FILE_PATH
            case _:
                raise ValueError

        return os.path.join(base_file_path, f"{TIME_INIT}.{LOG_OUTPUT_FILE_EXTENSION}")

    def get_target_logger(self, log_name, file_name, level=logging.INFO):
        handler = logging.FileHandler(file_name)
        specified_logger = logging.getLogger(log_name)
        specified_logger.setLevel(level)
        specified_logger.addHandler(handler)
        return specified_logger

    def get_logger(self, log_type):
        log_name = self.get_log_name(log_type)
        file_name = self.get_log_file_name(log_type)
        return self.get_target_logger(log_name, file_name)

    def log(self, message: str):
        # logger = self.get_logger(log_type)
        self.logger.info(message)

    def log_error(self, message: str):
        # logger = self.get_logger(log_type)
        self.logger.error(message)

    def log_alert(self, message: str):
        # logger = self.get_logger(log_type)
        self.logger.warning(message)


# LoggerSingleton = Logger()
