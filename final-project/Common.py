import copy
from enum import Enum

# from filters.BloomFilter import BloomFilter
from Logger import Logger, LogType


class OrgType(Enum):
    HASH = 0
    ORDERED = 1
    HEAP = 2


class AggregateType(Enum):
    COUNT = 0


class AuxiliaryType(Enum):
    B_PLUS_TREE = 0
    R_TREE = 1
    PRIMARY = 2
    CLUSTERED = 3
    SECONDARY = 4


class FilterType(Enum):
    BLOOM = 0


class RecordOrganizationType(Enum):
    ROW = 0
    COLUMN = 1


class Record:
    def __init__(self, coffee_id, coffee_name, intensity, country_of_origin) -> None:
        self.coffee_id = coffee_id
        self.coffee_name = coffee_name
        self.intensity = intensity
        self.country_of_origin = country_of_origin

    def tupl_mode(self):
        return (
            self.coffee_id,
            self.coffee_name,
            self.intensity,
            self.country_of_origin,
        )

    def __str__(self):
        return f"({self.coffee_id}, {self.coffee_name}, {self.intensity}, {self.country_of_origin})"


class Component:
    def __init__(self, log_type: LogType) -> None:
        self.log_type = log_type
        self.logger = Logger(log_type)

    def log(self, message):
        self.logger.log(message)


class AccessMethod:
    def __init__(self) -> None:
        return


class Auxiliary(AccessMethod):
    def __init__(self) -> None:
        super().__init__()


class Filter:
    def __init__(self) -> None:
        return


class Aggregate(AccessMethod):
    def set(self, key, aggregate_value):
        self.values[key] = aggregate_value

    def get(self, key):
        return self.values.get(key)


class Page:
    def __init__(self, _id, row_page=False) -> None:
        # print(f"\t\t\tMaking page {_id}")
        self.id = _id
        self.content = []
        self.map = {}  # each page has a map for getting tuples
        self.row_page = row_page
        self.max = 2

    def full(self):
        # print(
        # f"Page: {self.id} w len: {len(self.content)} and max {self.max} is full?: {len(self.content) == self.max}"
        # )
        return len(self.content) == self.max

    def get_row_tuple(self, t_id, c_id) -> tuple | None:
        if t_id not in self.map.keys():
            return None
        if c_id not in self.map[t_id].keys():
            return None
        else:
            return self.content[self.map[t_id][c_id]]

    def get_tuple(self, c_id) -> tuple | None:
        if self.row_page:
            t_id, c_id = c_id
            return self.get_row_tuple(t_id, c_id)
        if c_id in self.map.keys():
            # print("C_ID in  keys")
            idx = self.map[c_id]
            # print(f"Do an update c_id {c_id} to idx {idx} with content: {self.content}")
            return self.content[idx]
        else:
            # print(list(self.map.keys()))
            # print(f"C_ID not in keys: c_id {c_id}, page id: {self.id}")
            # print(f"coffee id {c_id} not in page: {self.id}")
            return None

    def add_row_tuple(self, t_id, record):
        if self.full():
            return
        c_id = record.coffee_id
        if t_id not in self.map.keys():
            self.map[t_id] = {}
        if c_id in self.map[t_id].keys():
            record_ = self.content[self.map[t_id][c_id]]
            # print(f"FOUND THE RECORD {record_}")
            # print(f"inserting: {record}")
            if record_.coffee_name is None:
                record_.coffee_name = record.coffee_name

            if record_.intensity is None:
                record_.intensity = record.intensity

            if record_.country_of_origin is None:
                record_.country_of_origin = record.country_of_origin
            # print(f"RECORD AFTER {record_}")
            return
        self.map[t_id][c_id] = len(self.content)
        self.content.append((t_id, record))

    def add_tuple(self, _tuple) -> None:
        # print("PAGE: adding a tuple")
        # will replace this w map for correct position in page
        if _tuple[0] in self.map.keys():
            # print(f"PAGE: c_id {_tuple[0]} is in map keys: {self.map.keys()}")
            if _tuple[1] is None:
                # print("PAGE: insert is a delete, val is none")
                idx = self.map[_tuple[0]]
                # print(f"Mapping is from c_id {_tuple[0]} to idx {idx}")
                # print("PAGE: remove tuple and update maps")
                del self.map[idx]
                self.content.remove(_tuple)

                # print(f"content after insert {self.content}")
                return 0
            else:
                # do an update
                idx = self.map[_tuple[0]]
                # print(
                # f"Do an update c_id {_tuple[0]} to idx {idx} with content: {self.content}"
                # )
                self.content[idx] = _tuple
                return 0
        # print("searching content")
        i = 0
        for tuple_ent in self.content:
            if tuple_ent[0] == _tuple[0]:
                # print(f"found tuple at {i}")
                tuple_ent[1] = _tuple[1]
                self.map[tuple_ent[0]] = i
                # print("update map")
                return 0
            i += 1
        if not self.full():
            self.map[_tuple[0]] = len(self.content)
            self.content.append(_tuple)
            # print(
            # f"added tuple at {len(self.content)-1} and updated map of c_id {_tuple[0]} to {len(self.content)-1}"
            # )
            return 0
        return -1

    def get_anchor(self):
        if len(self.content) == 0:
            return None
        else:
            return self.content[0]

    def __str__(self) -> str:
        return f"\n\t\tpage {self.id}: {[str(x) for x in self.content]}"


class File:
    def __init__(self, name, col_cache) -> None:
        # print(f"\t\tmaking file: {name}")
        self.attr = name
        self.page_map = {}  # page_num to page idx
        self.pages = []
        self.col_cache = col_cache

    def add_page(self, id) -> int:
        new_page = Page(id)
        self.pages.append(new_page)
        self.page_map[id] = len(self.pages) - 1
        return id

    def get_page(self, _id) -> Page:
        # do some check bounds etc
        return self.pages[self.page_map[_id]]

    def __str__(self) -> str:
        return f"\n\t{self.attr} " + "".join([str(x) for x in self.pages])


class Table:
    def __init__(self, name: str, PK: str, attrs: list, column_cache) -> None:
        # print(f"make table: {name}")
        self.name = name
        self.PK = PK
        self.attrs = {}
        for attr in attrs:
            # print(f"\tTABLE Making attr file: {attr}")
            self.attrs[attr] = File(attr, column_cache)

    def get_file(self, name):
        return self.attrs[name]

    def __str__(self):
        return f"{self.name} PK: {self.PK}" + "".join(
            [str(x) for x in self.attrs.values()]
        )


class OpType(Enum):
    B = "B"
    C = "C"
    A = "A"
    Q = "Q"
    I = "I"
    U = "U"
    R = "R"
    T = "T"
    M = "M"
    G = "G"


class Operation:
    def trim_operation_insert(self, line):
        elements = line.split("(")
        start = elements[0].split(" ")
        end = elements[1]
        args2 = [x.strip().replace(")", "") for x in end.split(",")]
        op = start[0]
        arg1 = start[1]
        return op, [arg1, args2]

    def to_txn_tuple(self, line):
        elements = line.split(" ")
        match (elements[0]):
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
                # print(elements[0])
                quit("Op Type Not recognized...")

    def __init__(self, line) -> None:
        self.op, self.args = self.to_txn_tuple(line)

    def is_write(self):
        return self.op in [OpType.I.value, OpType.U.value]

    def is_read(self):
        return self.op in [
            OpType.R.value,
            OpType.T.value,
            OpType.M.value,
            OpType.G.value,
        ]

    def is_commit(self):
        return self.op in [OpType.C.value]

    def get_resource(self):
        if self.is_write():
            return "w"
        elif self.is_read():
            return "r"
        elif self.is_commit():
            return "c"
        else:
            raise ValueError

    def __str__(self) -> str:
        match (self.op):
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


class Transaction:
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

    def __str__(self) -> str:
        return f"Transaction Id: {self.id}\n{[str(op) for op in self.ops]}"


class Lock:
    def __init__(self, tid, is_exclusive, resource, released=False):
        self.tid = tid
        self.is_exclusive = is_exclusive
        self.resource = resource
        self.released = released

    def format_as_history(self):
        operation = "l" if not self.released else "u"
        lock_type = "x" if self.exclusive else "s"
        return f"{operation}{lock_type}{self.transaction}[{self.resource}]"

    def __str__(self):
        return f"Lock - {self.transaction} - {self.exclusive} - {self.resource}"
