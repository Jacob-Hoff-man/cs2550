import os
import random
from enum import Enum

from Logger import LoggerSingleton as l


class OpType(Enum):
    B = 'B'
    C = 'C'
    A = 'A'
    Q = 'Q'
    I = 'I'
    U = 'U'
    R = 'R'
    T = 'T'
    M = 'M'
    G = 'G'

class Operation():
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
            case OpType.B.value:
                return (elements[0], elements[1])
            case OpType.C.value:
                return (elements[0], None)
            case OpType.A.value:
                return (elements[0], None)
            case OpType.Q.value:
                return (elements[0], None)
            case OpType.I.value:
                return self.trim_operation_insert(line)
            case OpType.U.value:
                return self.trim_operation_insert(line)
            case OpType.R.value:
                return (elements[0], list(elements[1:]))
            case OpType.T.value:
                return (elements[0], elements[1])
            case OpType.M.value:
                return (elements[0], elements[1:])
            case OpType.G.value:
                return (elements[0], elements[1])
            case _:
                print(elements[0])
                quit("Op Type Not recognized...")

    def __init__(self, line) -> None:
        self.op, self.args = self.to_txn_tuple(line)

    def __str__(self) -> str:
        match(self.op):
            case OpType.B.value:
                return f"{self.op} {self.args}"
            case OpType.C.value:
                return f"{self.op}"
            case OpType.A.value:
                return f"{self.op}"
            case OpType.Q.value:
                return f"{self.op}"
            case OpType.I.value:
                tuple_str = f"({self.args[1][0]}, {self.args[1][1]}, {self.args[1][2]}, {self.args[1][3]})"
                return f"{self.op} {self.args[0]} {tuple_str}"
            case OpType.U.value:
                return f"{self.op} {self.args[0]} ({self.args[1:]})"
            case OpType.R.value:
                return f"{self.op} {self.args[0]} {self.args[1]}"
            case OpType.T.value:
                return f"{self.op} {self.args[0]}"
            case OpType.M.value:
                return f"{self.op} {self.args[0]} {self.args[1]}"
            case OpType.G.value:
                return f"{self.op} {self.args[0]} {self.args[1]}"
            case _:
                return f"{self.op} {self.args}"
        

class Transaction():
    def __init__(self, id) -> None:
        self.id = id
        self.ops = []
    def add(self, op: Operation):
        self.ops.append(op)
    def get(self):
        return self.ops.pop(0)
    def __len__(self):
        return len(self.ops)
    def pop(self, idx):
        return self.ops.pop(idx)


class TransactionManager():
    def __init__(self) -> None:
        return

    def round_robin(self, txns) -> dict[str, any]:
        while len(txns.keys()) > 0:
            for file_name in list(txns):
                txn = txns[file_name]
                if len(txn) > 0:
                    oper = txn.pop(0) # one operation   
                    l.log(f"{os.path.splitext(os.path.split(file_name)[1])[0]}: {oper}")
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
                oper = txn.pop(0) # line   

                l.log(f"{os.path.splitext(os.path.split(file_name)[1])[0]}: {oper}")
            else:
                del txns[file_name]
                file_names.pop(rand_num)
    
    def read_files(self, raw_file_names):
        txns = {}
        for file_name in raw_file_names:
            _id = 0
            txn = Transaction(_id)

            with open(file_name) as f:
                for line in f:
                    txn.add(Operation(line.replace("\n", "")))
                    # make a new transaction
            txns[file_name] = txn

        return txns