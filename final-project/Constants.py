import os
import time

# Logger Constants
LOG_OUTPUT_FILE_EXTENSION = 'log'
TIME_STRING_FORMAT = '%Y%m%d-%H%M%S'
TIME_INIT = time.strftime(TIME_STRING_FORMAT)

# Log Type file paths
LOG_OUTPUT_FILE_PATH = 'logs/final-project/'
LOG_TRXN_MGR_OUTPUT_FILE_PATH = os.path.join(LOG_OUTPUT_FILE_PATH, 'transaction-manager')
LOG_DATA_MGR_OUTPUT_FILE_PATH = os.path.join(LOG_OUTPUT_FILE_PATH, 'data-manager')
LOG_CTLG_MGR_OUTPUT_FILE_PATH = os.path.join(LOG_OUTPUT_FILE_PATH, 'catalog-manager')
LOG_DBMS_SIM_OUTPUT_FILE_PATH = os.path.join(LOG_OUTPUT_FILE_PATH, 'dbms-simulator')