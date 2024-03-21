import os
import random
import sys

#import DeadlockDetector
from logger import LoggerSingleton as l

__LOG_TXN_MNGR_OUTPUT_FILE_PATH__ = 'logs/transaction-manager/'

##Round Robin (which reads one line from each file at a time in turns) 
def round_robin(txns):
    while len(txns.keys()) > 0:
        for file_name in list(txns):
            txn = txns[file_name]
            if len(txn) > 0:
                op = txn.pop(0) # line   
                l.log(f"{os.path.splitext(os.path.split(file_name)[1])[0]}: {op}")
            else:
                del txns[file_name]
            
                


##Random (which reads files in random order, and reads a random number of lines from each file).
def random_read(txns, seed = 42):
    file_names = list(txns.keys())
    random.seed(seed)

    while len(txns.keys()) > 0:
        rand_num = random.randrange(0, len(file_names))
        file_name = file_names[rand_num]
        txn = txns[file_name]
        if len(txn) > 0:
            op = txn.pop(0) # line   
            l.log(f"{os.path.splitext(os.path.split(file_name)[1])[0]}: {op}")
        else:
            del txns[file_name]
            file_names.pop(rand_num)

def read_file(file_name):
    file = open(file_name)
    return file

def read_files(raw_file_names):
    txns = {}
    for file_name in raw_file_names:
        txn = []
        with open(file_name) as f:
            for line in f:
                txn.append(line.replace("\n", ""))
        txns[file_name] = txn

    #file_names = [os.path.splitext(os.path.split(x)[1])[0] for x in raw_file_names]
    return txns

def main():
    l.config(__LOG_TXN_MNGR_OUTPUT_FILE_PATH__)
    #dld = DeadlockDector()

    txns = read_files(sys.argv[1:])

    #round_robin(txns)
    random_read(txns, 1234)
if __name__ == "__main__":
    main()


