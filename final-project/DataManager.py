import random

blm_flt = [0]*64000

class Page():
    def __init__(self, _id) -> None:
        print(f"\t\t\tMaking page {_id}")
        self.id = _id
        self.content = []
        self.map = {} # each page has a map for getting tuples
    def add_tuple(self, _tuple) -> None:
        self.content.append(_tuple)# will replace this w access method or something for correct position\

class File():
    def __init__(self, name) -> None:
        print(f"\t\tmaking file: {name}")
        self.attr = name
        self.map = {}
        self.pages = []
    def update_map(self, id, page_num):
        self.map[id] = page_num

    def remove_map(self, id):
        del self.map[id]
        
    def add_page(self) -> int:
        _id = len(self.pages)
        self.pages.append(Page(_id))
        return _id
    def get_page(self, _id) -> Page:
        # do some check bounds etc
        return self.pages[_id]
    
class Table():
    def __init__(self, name, PK, attrs) -> None:
        print(f"make table: {name}")
        self.name = name
        self.PK = PK
        self.attrs = {}
        for attr in attrs:
            print(f"\tMaking attr file: {attr}")
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
    
def LRU(col_cache: ColBuffer):
    evict = random.randrange(col_cache.max)
    col_cache.flush(evict)
    return evict

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

def update(table, ID, tuple_i):
    return True

def insert(table, ID, tuple_i):

    # check bloom filter for ID
    if blm_flt[ID] is 1: # if it is there
        return update(table, ID, tuple_i) # Call Update
    else: # if not
        if tuple_i is None: # if tuple is Null
            return 0 # no op 


    # check catalog for table
    if not (table in catalog.keys()):
        # if doesnt exist, Create
        catalog[table] = Table(table, 'CoffeeID', ['CoffeeName', 'Intensity', 'CountryOfOrigin'])
    
    # turn record into tuples
    # name = tuple_i[0]
    # intensity = tuple_i[1]
    # CoI = tuple_i[2]
    # name_tup = (ID, name)
    # inten_tup = (ID, intensity)
    # CoI_tup = (ID, CoI)

    tuples = ( ('CoffeeName', (ID, tuple_i[0])), ('Intensity', (ID, tuple_i[1])), ('CountryOfOrigin', (ID, tuple_i[2])))

    # for each tuple
    for attr, attr_tup in tuples:
        # use access method to get to page num for inserting 
        file = catalog[table].attrs[attr]
        page_num = file.map[attr_tup[0]]
        # check page table for page num
        entry_rec = pg_tbl.get_entry(page_num)
        if entry_rec is None: # not in the buffer
            # evict a page (LRU)
            frame_num = LRU(col_cache)
            # update page table w that page num and frame num
            entry = PageTableEntry(frame_num)
            pg_tbl.set_entry(page_num, entry)
            # put page in buffer at mapping
            #page = disc_mngr.get(File, page_num)
            page = file.get_page(page_num)
            
        else:

            page.add_tuple(attr_tup)
        # Now page is in buffer
        # use page map to find correct position for tuple in the page
        # insert the tuple
        # update the map
        # update the access method if need be 
        
    

    return True

col_cache = ColBuffer(4)
pg_tbl = PageTable()
catalog = {}
catalog['starbucks'] = Table("Starbucks", 'CoffeeID', ['CoffeeName', 'Intensity', 'CountryOfOrigin'])
    
