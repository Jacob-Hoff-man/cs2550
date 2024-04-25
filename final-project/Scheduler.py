import re
from DeadlockDetector import DeadlockDetector

class Lock():
    def __init__(self, transaction, exclusive, resource, released=False):
        self.transaction = transaction
        self.exclusive = exclusive
        self.resource = resource
        self.released = released

    def format_as_history(self):
        operation = 'l' if not self.released else 'u'
        lock_type = 'x' if self.exclusive else 's'
        return f'{operation}{lock_type}{self.transaction}[{self.resource}]'

    def __str__(self):
        return f'Lock - {self.transaction} - {self.exclusive} - {self.resource}'

class Scheduler():
    def execute(self, txns):
        self.operations = []
        self.delayed_operations = []
        self.transactions = {}
        self.locks = []
        self.execution_list = []
        self.final_history = []
        self.counter = 0
        self.dld = DeadlockDetector()

        self.parse_txns(txns)
        print('unserialized history: ')
        for op in self.operations:
            print('@@ op', op)
        # self.run_operations()
        print('--------------------')

    def parse_txns(self, txns):
        for txn in txns:
            self.transactions[txn.id] = txn
            for op in txn.ops:
                self.operations.append((txn.id, op))

    def has_delayed_op(self, tid):
        for op in self.delayed_operations:
            if op[0] == tid:
                return True
        return False
    
    def delay(self, op):
        self.delayed_operations.append(op)
        print(f'Operation Delayed: {op[1]} ')

    def can_grow_transaction(self, tid):
        return self.transactions[tid].is_growing

    # TODO
    # def run_operation(self, op):

    #     if op[1].is_write() or op[1].is_read():
    #         if self.can_grow_transaction(op[0]):

    #     elif: op.is_commit():

    # TODO
    # def run_operations(self):
    #     self.execution_list = self.operations
    #     while self.counter < len(self.execution_list):
    #         if self.has_delayed_operation(self.execution_list[self.counter]):
    #             self.delay(self.execution_list[self.counter])
    #         else:
    #             op = self.run_operation(self.execution_list[self.counter])
    #             if op:
    #                 self.delay(op)
    #                 # dl = 