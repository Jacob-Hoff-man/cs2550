import os
import time

# Logger Constants
__LOG_OUTPUT_FILE_EXTENSION__ = 'log'
__TIME_STRING_FORMAT__ = '%Y%m%d-%H%M%S'
__TIME_INIT__ = time.strftime(__TIME_STRING_FORMAT__)

# Log Type file paths
__LOG_OUTPUT_FILE_PATH__ = 'logs/final-project/'
__LOG_TRXN_MGR_OUTPUT_FILE_PATH__ = os.path.join(__LOG_OUTPUT_FILE_PATH__, 'transaction-manager')
__LOG_DATA_MGR_OUTPUT_FILE_PATH__ = os.path.join(__LOG_OUTPUT_FILE_PATH__, 'data-manager')
__LOG_CTLG_MGR_OUTPUT_FILE_PATH__ = os.path.join(__LOG_OUTPUT_FILE_PATH__, 'catalog-manager')
__LOG_DBMS_SIM_OUTPUT_FILE_PATH__ = os.path.join(__LOG_OUTPUT_FILE_PATH__, 'dbms-simulator')