#import DeadlockDetector
from TransactionManager import TransactionManager
from CatalogManager import CatalogManager
import sys
from Logger import LoggerSingleton as l

__LOG_TXN_MNGR_OUTPUT_FILE_PATH__ = 'logs/final-project/transaction-manager/'

def main():
    #dld = DeadlockDector()
    schema_file_name = sys.argv[1]
    txn_processing_type = sys.argv[2]
    file_names = sys.argv[3:]
    l.config(__LOG_TXN_MNGR_OUTPUT_FILE_PATH__, f'-{type}')
    txn_mgr = TransactionManager()
    catalog_mgr = CatalogManager(schema_file_name)
    catalog_mgr.insert_catalog('table_key')

    # txns = txn_mgr.read_files(file_names)

    if txn_processing_type == 'rr':
        txn_mgr.round_robin(txns)
    elif txn_processing_type == 'ran':
        txn_mgr.random_read(txns)


if __name__ == "__main__":
    main()
