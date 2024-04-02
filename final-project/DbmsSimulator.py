#import DeadlockDetector
from TransactionManager import TransactionManager
import sys
from logger import LoggerSingleton as l

__LOG_TXN_MNGR_OUTPUT_FILE_PATH__ = 'logs/final-project/transaction-manager/'

def main():
    #dld = DeadlockDector()

    type = sys.argv[1]
    l.config(__LOG_TXN_MNGR_OUTPUT_FILE_PATH__, f'-{type}')
    file_names = sys.argv[2:]
    txn_mgr = TransactionManager()
    txns = txn_mgr.read_files(file_names)
    if type == 'rr':
        txn_mgr.round_robin(txns)
    elif type == 'ran':
        txn_mgr.random_read(txns)


if __name__ == "__main__":
    main()
