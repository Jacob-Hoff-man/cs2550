import os
import random
from enum import Enum

from logger import LoggerSingleton as l


class OpType(Enum):
    B = 'B'
    R = 'R'
    U = 'U'
    I = 'I'
    T = 'T'
    M = 'M'
    G = 'G'

class Transaction():
    def trim_operation_insert(self, line):
        elements = line.split('(')
        start = elements[0].split(' ')
        end = elements[1]
        args2 = [x.strip().replace(')', '') for x in end.split(',')]
        op = start[0]
        arg1 = start[1]
        return op, [arg1, args2]

    def to_txn_tuple(self, line):
        elements = line.split(' ')
        match(elements[0]):
            case OpType.B:
                return (elements[0], elements[1])
            case OpType.R:
                return (elements[0], list(elements[1:]))
            case OpType.U:
                return self.trim_operation_insert(line)
            case OpType.I:
                return self.trim_operation_insert(line)
            case OpType.T:
                return (elements[0], elements[1])
            case OpType.M:
                return (elements[0], elements[1:])
            case OpType.G:
                pass
            case _:
                quit("Op Type Not recognized...")

    def __init__(self, line) -> None:
        self.op, self.args = self.to_txn_tuple(line)

class TransactionManager():
    def __init__(self) -> None:
        return

    def round_robin(self, txns) -> dict[str, any]:
        while len(txns.keys()) > 0:
            for file_name in list(txns):
                txn = txns[file_name]
                if len(txn) > 0:
                    op = txn.pop(0) # line   
                    txn = Transaction(op)

                    l.log(f"{os.path.splitext(os.path.split(file_name)[1])[0]}: {txn.op}{txn.args}")
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
                txn = Transaction(op)

                l.log(f"{os.path.splitext(os.path.split(file_name)[1])[0]}: {txn.op}{txn.args}")
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
                    # make a new transaction
            txns[file_name] = Transaction(txn)
            #txns[file_name] = txn

        return txns