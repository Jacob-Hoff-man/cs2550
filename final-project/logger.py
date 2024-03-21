import logging
import time

__LOG_OUTPUT_FILE_EXTENSION__ = f'log'
__TIME_STRING_FORMAT__ = f'%Y%m%d-%H%M%S'

class Logger():
    def config(self, file_path: str) -> None:
        file_name = f'{file_path}{time.strftime(__TIME_STRING_FORMAT__)}.{__LOG_OUTPUT_FILE_EXTENSION__}' 
        with open(file_name, 'w') as fp: 
            pass
        logging.basicConfig(
            filename = file_name,
            level = logging.DEBUG,
        )

    def log(self, message: str):
        logging.info(message)
    
    def log_error(self, message: str):
        logging.error(message)

    def log_alert(self, message: str):
        logging.warning(message)
        
LoggerSingleton = Logger()