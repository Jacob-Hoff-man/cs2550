import copy
import random

from Common import Component
from Logger import LogType

blm_flt = [0]*64000

global num_pages
num_pages = 0


class Page():
    def __init__(self, _id) -> None:
        print(f"\t\t\tMaking page {_id}")
        self.id = _id
        self.content = []
        self.map = {}  # each page has a map for getting tuples
        self.max = 2

    def full(self):
        print(
            f"Page: {self.id} w len: {len(self.content)} and max {self.max} is full?: {len(self.content) == self.max}")
        return len(self.content) == self.max

    def add_tuple(self, _tuple) -> None:
        print("PAGE: adding a tuple")
        # will replace this w map for correct position in page
        if _tuple[0] in self.map.keys():
            print(f"PAGE: c_id {_tuple[0]} is in map keys: {self.map.keys()}")
            if _tuple[1] is None:
                print("PAGE: insert is a delete, val is none")
                idx = self.map[_tuple[0]]
                print(f"Mapping is from c_id {_tuple[0]} to idx {idx}")
                del self.map[idx]

                print("PAGE: remove tuple and update maps")
                temp = []
                for _tup in self.content:
                    if _tup[0] is not _tuple[0]:
                        print(f"keep {_tup[0]}")
                        temp.append(_tup)
                        self.map[_tup[0]] = len(temp)
                    else:
                        print(f"removed {_tup[0]}")
                self.content = copy.deepcopy(temp)
                print(f"content after insert {self.content}")
                return
            else:
                # do an update
                idx = self.map[_tuple[0]]
                print(
                    f"Do an update c_id {_tuple[0]} to idx {idx} with content: {self.content}")
                self.content[idx] = _tuple
                return
        print("searching content")
        i = 0
        for tuple_ent in self.content:
            if tuple_ent[0] == _tuple[0]:
                print(f"found tuple at {i}")
                tuple_ent[1] = _tuple[1]
                self.map[tuple_ent[0]] = i
                print("update map")
                return
            i += 1

        self.map[_tuple[0]] = len(self.content)
        self.content.append(_tuple)
        print(
            f"added tuple at {len(self.content)} and updated map of c_id {_tuple[0]} to {len(self.content)}")

    def __str__(self) -> str:
        return f"\n\t\tpage {self.id}: {self.content}"


class File():
    def __init__(self, name) -> None:
        print(f"\t\tmaking file: {name}")
        self.attr = name
        self.map = {}
        self.page_map = {}
        self.pages = []

    def get_map(self, id):
        if id not in self.map.keys():
            # id is not in the file yet
            print(f"id {id} is not in file map yet ")
            # return the best page for it to be on
            for page in self.pages:
                if pg_tbl.get_entry(page.id).valid:
                    print("page found is in buffer")
                    if not col_cache.get(pg_tbl.get_entry(page.id).frame_num).full():
                        print("found an empty page in buffer")
                        # page isnt full
                        return page.id
                else:
                    print("page is not in buffer")
                    if not page.full():
                        print("found an empty page in disk")
                        # page isnt full
                        return page.id
            # all pages are full
            print("all pages are full or no pages exists yet")
            return None
        else:
            return self.map[id]

    def update_map(self, id, page_num) -> None:
        self.map[id] = page_num

    def remove_map(self, id) -> None:
        del self.map[id]

    def add_page(self) -> int:
        global num_pages
        print(f"FILE num pages is {num_pages}")
        _id = num_pages
        num_pages += 1

        new_page = Page(_id)
        self.pages.append(new_page)
        self.page_map[_id] = len(self.pages)-1
        return _id

    def get_page(self, _id) -> Page:
        # do some check bounds etc
        return self.pages[self.page_map[_id]]

    def __str__(self) -> str:
        return f"\n\t{self.attr} " + "".join([str(x) for x in self.pages])


class Table():
    def __init__(self, name, PK, attrs) -> None:
        print(f"make table: {name}")
        self.name = name
        self.PK = PK
        self.attrs = {}
        for attr in attrs:
            print(f"\tTABLE Making attr file: {attr}")
            self.attrs[attr] = File(attr)

    def get_file(self, name):
        return self.attrs[name]

    def __str__(self):
        return f"{self.name} PK: {self.PK}" + "".join([str(x) for x in self.attrs.values()])


class PageTableEntry:
    def __init__(self, frame_num):
        self.dirty = False
        self.refer = False
        self.valid = False
        self.frame_num = frame_num

    def __str__(self) -> str:
        return f"{self.dirty} {self.refer} {self.valid} {self.frame_num}"


class PageTable:
    def __init__(self):
        self.map = {}

    def get_entry(self, page_num):
        if page_num not in self.map.keys():
            # NO MAPPING
            return None
        else:
            # return mapping
            # print(self.map.keys())
            return self.map[page_num]

    def set_entry(self, page_num, page_table_entry):
        self.map[page_num] = page_table_entry


def write_out(page: Page):

    pg_id = page.id
    print(f"writing out page w id {pg_id}")

    for table in catalog.values():
        for file in table.attrs.values():
            i = 0
            for page_ in file.pages:
                if page_.id == pg_id:
                    print("Found the page on disc")
                    file.pages[i] = copy.deepcopy(page)
                    return
                i += 1
    print("page wasnt found on disk")


class ColBuffer():
    def __init__(self, size) -> None:
        self.buffer = {}
        self.max = size  # max index in buffer (used by page table)
        self.free = [x for x in range(self.max)]

    def do_op(self, op):
        # data manager will send ops to buffer such as flushing
        # or sending to row buffer
        return

    def full_flush(self):
        leng = len(self.buffer)
        for i in range(leng):
            self.flush(i)
        self.free = [x for x in range(self.max)]

    def flush(self, idx):
        # flushes this frame
        # get entry using frame num
        print(f"Flushing frame: {idx}")
        page = self.buffer[idx]
        print(f"page selected is {page.id}")
        pg_entry = pg_tbl.get_entry(page.id)
        print(f"got entry from page table: {pg_entry}")
        pg_entry.valid = False
        print("make mapping false")
        if pg_entry.dirty:
            print("page is dirty, write it out")
            write_out(page)
        print(f"deleting {idx} from {self.buffer.keys()}")
        del self.buffer[idx]
        self.free.append(idx)
        self.free.sort()

    def set(self, idx, page):
        # sets idx to page
        # we're doing a shallow copy. Need a deep copy
        try:
            # if len(self.buffer) < self.max:
            #     last = len(self.buffer)
            #     self.buffer[last] = copy.deepcopy(page)
            # else:
            self.buffer[idx] = copy.deepcopy(page)
            print(f"COL BUFFER Successful set {idx} to {page.id}")
            return True
        except:
            print(
                f"COL BUFFER failure to add {page.id} at {idx} with buffer len: {len(self.buffer)}")
            return False

    def get(self, idx):
        print(f"COL BUFFER getting {idx} from {len(self.buffer)} size buffer")
        if idx in self.buffer.keys():
            return self.buffer[idx]
        print(f"COL BUFFER idx not in buffer: {self.buffer.keys()}")
        return None

    def to_row(self, idx):
        # create the record
        # return it to data manager or can send to directly to row buffer
        return

    def __str__(self) -> str:
        if len(self.buffer) == 0:
            print("COLUMN BUFFER EMPTY")
        return "BUFFER: " + "".join([str(x) for x in self.buffer.values()])


def lru():
    # need a way to know that there is an invalid mapping and can therefore be overwritten
    print(f'LRU: len: {len(col_cache.buffer)} max: {col_cache.max}')
    if len(col_cache.free) > 0:
        # empty slot!
        # fill it.
        idx = col_cache.free.pop(0)
        print(f"LRU: empty slot in buffer return {idx}")
        return idx
    try:
        evict = random.randrange(len(col_cache.buffer))
        col_cache.flush(evict)
        print(f"LRU: chose {evict} to evict")
        return evict
    except:
        print("LRU: exception occured")
        col_cache.flush(0)
        return 0


class RowBuffer():
    def __init__(self, size) -> None:
        self.buffer = {}
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


class DataManager(Component):
    def __init__(self) -> None:
        super().__init__(LogType.DATA_MANAGER)

        # self.recovery_log = {}
        # self.catalog = {}
        # self.page_table = PageTable()
        # self.column_buffer = {}
        # self.row_buffer = {}

        # self.log('data manager component intialized message')

    def do_op(self, op):
        # does an operation
        # switch case
        # helper for each op

        return

    def get_txn_ops_from_rl(self):
        raise NotImplementedError

    def is_op_in_page_table(self):  # I, U, R, M
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


def update(table, ID, val):
    raise NotImplementedError
    print("UPDATE UPDATEing a tuple")

    if tuple_i == None and blm_flt[ID] == 0:  # if tuple is Null
        return 0  # no op

    # check catalog for table

    if table not in catalog.keys():
        print("UPDATE Create table")
        # if doesnt exist, Create
        catalog[table] = Table(table, 'CoffeeID', [
                               'CoffeeName', 'Intensity', 'CountryOfOrigin'])

    # turn record into tuples
    print("UPDATE Create tuples from record")
    tuples = (('CoffeeName', (ID, tuple_i[0])), ('Intensity',
              (ID, tuple_i[1])), ('CountryOfOrigin', (ID, tuple_i[2])))

    # for each tuple
    table = catalog[table]
    for attr, attr_tup in tuples:
        print("\n")

        print(f"UPDATE on {attr} with tuple: {attr_tup}")
        # use access method to get to page num for UPDATEing
        c_id = attr_tup[0]
        # val = attr_tup[1]
        file = table.attrs[attr]
        print(f"UPDATE c_id: {c_id}")
        page_num = file.get_map(c_id)
        if page_num is None:
            print("UPDATE page num is none add a page")
            # make a page
            page_num = file.add_page()
            print(
                f"UPDATE update the map c_id: {c_id} to page_num: {page_num}")
            file.update_map(c_id, page_num)

        # check page table for page num
        print(f"UPDATE get entry from page table for page_num {page_num}")
        entry_rec = pg_tbl.get_entry(page_num)
        if entry_rec is None or entry_rec.valid == False:  # not in the buffer
            # evict a page (LRU)
            print("UPDATE entry_rec is None\nCall LRU")
            frame_num = lru()
            print(f"UPDATE evicted {frame_num}")
            # update page table w that page num and frame num
            print("UPDATE create page table entry")
            entry = PageTableEntry(frame_num)
            print(f"UPDATE set page table entry {page_num} {entry} ")
            pg_tbl.set_entry(page_num, entry)
            entry.valid = True
            entry.dirty = True
            # put page in buffer at mapping
            # page = disc_mngr.get(File, page_num)
            print(f"UPDATE set page from file page num: {page_num}")
            page = file.get_page(page_num)
            print(f"UPDATE set cache value {frame_num} {page_num}")
            col_cache.set(frame_num, page)
            # overwrite old page this is like pointer stuff. we want the deep copy version thats unchanged until we flush
            page = col_cache.get(frame_num)
            print("UPDATE col cache after set: ", col_cache)
            if page is None:
                quit("UPDATE 1: page is none")
        else:
            print(f"UPDATEget page from cache {entry_rec.frame_num}")
            page = col_cache.get(entry_rec.frame_num)
            entry_rec.dirty = True
            entry_rec.valid = True

            if page is None:
                quit("UPDATE 2: page is none")
        # Now page is in buffer
        # use page map to find correct position for tuple in the page

        # UPDATE the tuple
        print("UPDATE UPDATE tuple to page")
        page.add_tuple(attr_tup)
        print("UPDATE col cache after tuple UPDATE: ", col_cache)
        # update the map
        print(f"UPDATE update file map {c_id} {page_num}")
        file.update_map(c_id, page_num)
        # update the access method if need be
        # update the bloom filter
        blm_flt[c_id] = 1
        print("\n")
    print(col_cache)
    return True


def insert(table, ID, tuple_i):
    print("INSERT inserting a tuple")
    # check bloom filter for ID
    # if blm_flt[ID] == 1: # if it is there
    #     print("ID present in data (probably) updating...")
    #     return update(table, ID, tuple_i) # Call Update
    # else: # if not
    #     print("ID not present in data")

    if tuple_i == None and blm_flt[ID] == 0:  # if tuple is Null
        return 0  # no op

    # check catalog for table

    if table not in catalog.keys():
        print("INSERT Create table")
        # if doesnt exist, Create
        catalog[table] = Table(table, 'CoffeeID', [
                               'CoffeeName', 'Intensity', 'CountryOfOrigin'])

    # turn record into tuples
    print("INSERT Create tuples from record")
    tuples = (('CoffeeName', (ID, tuple_i[0])), ('Intensity',
              (ID, tuple_i[1])), ('CountryOfOrigin', (ID, tuple_i[2])))

    # for each tuple
    table = catalog[table]
    for attr, attr_tup in tuples:
        print("\n")

        print(f"INSERT on {attr} with tuple: {attr_tup}")
        # use access method to get to page num for inserting
        c_id = attr_tup[0]
        # val = attr_tup[1]
        file = table.attrs[attr]
        print(f"INSERT c_id: {c_id}")
        page_num = file.get_map(c_id)
        if page_num is None:
            print("INSERT page num is none add a page")
            # make a page
            page_num = file.add_page()
            print(
                f"INSERT update the map c_id: {c_id} to page_num: {page_num}")
            file.update_map(c_id, page_num)

        # check page table for page num
        print(f"INSERT get entry from page table for page_num {page_num}")
        entry_rec = pg_tbl.get_entry(page_num)
        if entry_rec is None or entry_rec.valid == False:  # not in the buffer
            # evict a page (LRU)
            print("INSERT entry_rec is None\nCall LRU")
            frame_num = lru()
            print(f"INSERT evicted {frame_num}")
            # update page table w that page num and frame num
            print("INSERT create page table entry")
            entry = PageTableEntry(frame_num)
            print(f"INSERT set page table entry {page_num} {entry} ")
            pg_tbl.set_entry(page_num, entry)
            entry.valid = True
            entry.dirty = True
            # put page in buffer at mapping
            # page = disc_mngr.get(File, page_num)
            print(f"INSERT set page from file page num: {page_num}")
            page = file.get_page(page_num)
            print(f"INSERT set cache value {frame_num} {page_num}")
            col_cache.set(frame_num, page)
            # overwrite old page this is like pointer stuff. we want the deep copy version thats unchanged until we flush
            page = col_cache.get(frame_num)
            print("INSERT col cache after set: ", col_cache)
            if page is None:
                quit("INSERT 1: page is none")
        else:
            print(f"INSERTget page from cache {entry_rec.frame_num}")
            page = col_cache.get(entry_rec.frame_num)
            entry_rec.dirty = True
            entry_rec.valid = True

            if page is None:
                quit("INSERT 2: page is none")
        # Now page is in buffer
        # use page map to find correct position for tuple in the page

        # insert the tuple
        print("INSERT insert tuple to page")
        page.add_tuple(attr_tup)
        print("INSERT col cache after tuple insert: ", col_cache)
        # update the map
        print(f"INSERT update file map {c_id} {page_num}")
        file.update_map(c_id, page_num)
        # update the access method if need be
        # update the bloom filter
        blm_flt[c_id] = 1
        print("\n")
    print(col_cache)
    return True


dt_mngr = DataManager()
col_cache = ColBuffer(4)
pg_tbl = PageTable()
catalog = {}
catalog['starbucks'] = Table("Starbucks", 'CoffeeID', [
                             'CoffeeName', 'Intensity', 'CountryOfOrigin'])

insert('starbucks', 0, ('latte', 5, 'USA'))
print(catalog['starbucks'])  # table print
print(col_cache)

insert('starbucks', 0, ('mochiato', 10, 'France'))
print(catalog['starbucks'])  # table print


insert('starbucks', 1, ('nitro', 12, 'USA'))
print(catalog['starbucks'])  # table print

col_cache.flush(0)
print(catalog['starbucks'])  # table print
print(col_cache)

insert('starbucks', 2, ('latte', 5, 'ITALLIIIAAA'))
print(catalog['starbucks'])  # table print
print(col_cache)

col_cache.full_flush()
print(catalog['starbucks'])  # table print
print(col_cache)
