import re
from DeadlockDetector import DeadlockDetector
from Common import Operation, Lock

class Scheduler():
    def __init__(self, txns):
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
        print('--------------------')
        self.get_serialized_history()

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

    def can_commit(self, tid):
        ops = [op for op in self.delayed_operations if op[0] == tid]
        return len(ops) == 0
    
    def abort_tid(self, tid):
        self.delayed_operations = [op for op in self.delayed_operations if op.tid != tid]
        self.final_history = [op for op in self.final_history if op.tid != tid]
        self.locks = [lock for lock in self.locks if lock.tid != tid]

        count_decrement = 0
        for index, op in enumerate(self.execution_list):
            if op[0] == tid and index < self.counter:
                count_decrement += 1
        self.execution_list = [op for op in self.execution_list if op[0] != tid]
        self.counter -= count_decrement
        for op in self.operations:
            if op[0] == tid:
                self.execution_list.append(op)

    def insert_lock(self, op):
        is_exclusive = True if op[1].is_write() else False
        lock = Lock(op[0], is_exclusive, op[1].resource())
        self.locks.append(lock)

    def remove_locks(self, tid):
        original_locks = list(self.locks)
        self.locks[:] = [lock for lock in self.locks if lock.tid != tid]
        for released_lock in set(original_locks).difference(set(self.locks)):
            lock = Lock(released_lock.tid, released_lock.exclusive, released_lock.resource(), True)
            self.final_history.append(lock)
        
    def has_lock(self, op):
        for lock in self.locks:
            is_tid_match = lock.tid == op[0]
            is_resource_match = lock.resource == op[1].resource()
            is_proper_lock = ((lock.exclusive and op[1].is_write()) or (not lock.exclusive and op[1].is_read()))
            if is_tid_match and is_resource_match and is_proper_lock:
                return True
        return False

    def can_lock(self, op):
        matching_locks = [lock for lock in self.locks if lock.resource == op[1].resource()]
        for lock in matching_locks:
            if not lock.exclusive:
                if lock.tid == op[0] and op[1].is_write() and op[1] and len(matching_locks) == 1:
                    return True
                elif lock.tid != op[0] and op[1].is_read():
                    return True
            return False
        return True
    
    def has_dl(self):
        conflicts = []
        for op in self.delayed_operations:
            for lock in self.locks:
                if op[0] != lock.tid and op[1].get_resource() == lock.resource:
                    conflicts.append((lock.tid, op[0]))
        for waiting, waiting_for in conflicts:
            dl = self.dld.wait_for(waiting, waiting_for)
            if dl:
                return dl
            else:
                return False

    def has_delayed_operation(self, tid):
        delayed_ops = [op for op in self.delayed_operations if op[0] == tid]
        if len(delayed_ops) > 0:
            return True
        return False

    def run_delayed_operations(self):
        if self.delayed_operations:
            ops = []
            for delayed_op in self.delayed_operations:
                op = self.run_operation(delayed_op)
                if op:
                    ops.append(op)
            self.delayed_operations = ops

    def run_operation(self, op):

        if op[1].is_write() or op[1].is_read():
            if self.can_grow_transaction(op[0]):
                if self.has_lock(op):
                    self.final_history.append(op)
                elif self.can_lock(op):
                    self.insert_lock(op)
                    self.final_history.append(op)
                else:
                    return op
        elif op[1].is_commit() and self.can_commit(op[0]):
                self.final_history.append(op)
                self.remove_locks(op[0])
                self.transactions[op[0]].is_growing = False

    def get_serialized_history(self):
        self.execution_list = self.operations
        while self.counter < len(self.execution_list):
            if self.has_delayed_operation(self.execution_list[self.counter]):
                self.delay(self.execution_list[self.counter])
            else:
                op = self.run_operation(self.execution_list[self.counter])
                if op:
                    self.delay(op)
                    dl = self.has_deadlock()
                    if dl:
                        self.abort_tid(op[0])
                self.run_delayed_operations()
            self.counter += 1
            if self.counter == len(self.execution_list):
                    for op in self.delayed_operations:
                        self.execution_list.append(op)
                    self.delayed_operations = []

        return self.final_history

    def print_final_history(self):
        operations_text = ''
        for op in self.final_history:
            if isinstance(op, Operation) or isinstance(op, Lock):
                operations_text += f'{op}, '
        if operations_text:
            print(f'Final history: {operations_text.strip(", ")}')

        
