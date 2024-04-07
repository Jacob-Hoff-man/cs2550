import logging
import os
from enum import Enum
from Constants import (
    __LOG_CTLG_MGR_OUTPUT_FILE_PATH__,
    __LOG_DATA_MGR_OUTPUT_FILE_PATH__,
    __LOG_DBMS_SIM_OUTPUT_FILE_PATH__,
    __LOG_TRXN_MGR_OUTPUT_FILE_PATH__,
    __LOG_OUTPUT_FILE_EXTENSION__,
    __TIME_STRING_FORMAT__,
    __TIME_INIT__
)

class LogType(Enum):
    CATALOG_MANAGER = 'Catalog Manager'
    DATA_MANAGER = 'Data Manager'
    DBMS_SIMULATOR = 'DBMS Simulator'
    TRANSACTION_MANAGER = 'Transaction Manager'

class Logger():
 
    def get_log_name(self, log_type: LogType):
        return log_type.value

    def get_log_file_name(self, log_type: LogType):
        match log_type:
            case LogType.CATALOG_MANAGER:
                base_file_path = __LOG_CTLG_MGR_OUTPUT_FILE_PATH__
            case LogType.DATA_MANAGER:
                base_file_path = __LOG_DATA_MGR_OUTPUT_FILE_PATH__
            case LogType.DBMS_SIMULATOR:
                base_file_path = __LOG_DBMS_SIM_OUTPUT_FILE_PATH__
            case LogType.TRANSACTION_MANAGER:
                base_file_path = __LOG_TRXN_MGR_OUTPUT_FILE_PATH__
            case _:
                raise ValueError
            
        return os.path.join(base_file_path, f'{__TIME_INIT__}.{__LOG_OUTPUT_FILE_EXTENSION__}')
        
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

    def log(self, log_type: LogType, message: str):
        logger = self.get_logger(log_type)
        logger.info(message)
    
    def log_error(self, log_type: LogType, message: str):
        logger = self.get_logger(log_type)
        logger.error(message)

    def log_alert(self, log_type: LogType, message: str):
        logger = self.get_logger(log_type)
        logger.warning(message)

LoggerSingleton = Logger()