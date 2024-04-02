import os
import random
from logger import LoggerSingleton as l

class TransactionManager():
    def __init__(self) -> None:
        pass

    def round_robin(self, txns) -> dict[str, any]:
        while len(txns.keys()) > 0:
            for file_name in list(txns):
                txn = txns[file_name]
                if len(txn) > 0:
                    op = txn.pop(0) # line   
                    l.log(f"{os.path.splitext(os.path.split(file_name)[1])[0]}: {op}")
                else:
                    del txns[file_name]
    
    def random_read(self, txns, seed = 42):
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
    
    def read_files(self, raw_file_names):
        txns = {}
        for file_name in raw_file_names:
            txn = []
            with open(file_name) as f:
                for line in f:
                    txn.append(line.replace("\n", ""))
            txns[file_name] = txn

        return txns