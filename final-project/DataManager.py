class Page():
    def __init__(self, _id) -> None:
        self.id = _id
        self.content = []
        self.map = {} # each page has a map for getting tuples
    def add_tuple(self, _tuple) -> None:
        self.content.append(_tuple)# will replace this w access method or something for correct position\

class File():
    def __init__(self, name) -> None:
        self.attr = name
        self.pages = []
    def add_page(self) -> None:
        _id = len(self.pages)
        self.pages.append(Page(_id))
    def get_page(self, _id) -> Page:
        # do some check bounds etc
        return self.pages[_id]
    
class Table():
    def __init__(self, name, PK, attrs) -> None:
        self.name = name
        self.PK = PK
        self.attrs = {}
        for attr in attrs:
            self.attrs[attr] = File(attr)

    def get_file(self, name):
        return self.attrs[name]

class PageTableEntry:
    def __init__(self, frame_num):
        self.dirty = False
        self.refer = False
        self.valid = False
        self.frame_num = frame_num

class PageTable:
    def __init__(self):
        self.map = {}

    def get_entry(self, page_num):
        if page_num >= len(self.map.keys()): 
            # NO MAPPING
           return None
        else:
            # return mapping
            return self.map[page_num]

    def set_entry(self, page_num, page_table_entry):
        self.map[page_num] = page_table_entry

class ColBuffer():
    def __init__(self, size) -> None:
        self.buffer = []
        self.max = size - 1 # max index in buffer (used by page table)

    def do_op(self, op):
        # data manager will send ops to buffer such as flushing 
        # or sending to row buffer
        return
    def flush(self, idx):
        # flushes this frame
        # if dirty need to write back/through to disc
        return
    def set(self, idx, page):
        # sets idx to page
        return
    def to_row(self, idx):
        # create the record
        # return it to data manager or can send to directly to row buffer
        return

class RowBuffer():
    def __init__(self, size) -> None:
        self.buffer = [None]*size
        self.max = size
    def do_op(self, op):
        # data manager will send ops to buffer such as flushing 
        # or sending to row buffer
        return
    def flush(self, idx):
        # flushes this frame
        # if dirty need to write back/through to disc
        return
    def set(self, idx, page):
        # sets idx to page
        return
    def to_col(self, idx):
        # create the columns
        # return it to data manager or can send to directly to col buffer
        return

class DataManager():
    def __init__(self, tb) -> None:
        self.tb = None
        self.attributes = None
        # self.recovery_log = {}
        # self.catalog = {}
        # self.page_table = PageTable()
        # self.column_buffer = {}
        # self.row_buffer = {}
        
        
    def do_op(self, op):
        # does an operation
        # switch case
        # helper for each op
        return
    
    def get_txn_ops_from_rl(self):
        raise NotImplementedError

    def is_op_in_page_table(self): # I, U, R, M
        # get CoffeeID from Operation Args
        # check bloom filter for CoffeeID (insert will be false unless deleting)
        # 
        raise NotImplementedError

    def convert_op_to_col(self):
        raise NotImplementedError

    def convert_op_to_row(self):
        raise NotImplementedError

    def convert_row_to_col(self):
        raise NotImplementedError

    def convert_col_to_row(self):
        raise NotImplementedError

    def use_access_method(self):
        raise NotImplementedError

    def insert_to_disc(self):
        raise NotImplementedError

tbl_stb = Table("Starbucks", 'CoffeeID', ['Name', 'Intensity', 'CountryOfOrigin'])

    
