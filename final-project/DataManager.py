import copy
import random

from CatalogManager import CatalogManager
from Common import Component, File, Page, Record, Table

# from Common import Component
from Logger import LogType

blm_flt = [0] * 64000

num_pages = 0


class page_old:
    def __init__(self, _id) -> None:
        print(f"\t\t\tMaking page {_id}")
        self.id = _id
        self.content = []
        self.map = {}  # each page has a map for getting tuples
        self.max = 2

    def full(self):
        print(
            f"Page: {self.id} w len: {len(self.content)} and max {self.max} is full?: {len(self.content) == self.max}"
        )
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
                    f"Do an update c_id {_tuple[0]} to idx {idx} with content: {self.content}"
                )
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
            f"added tuple at {len(self.content)} and updated map of c_id {_tuple[0]} to {len(self.content)}"
        )

    def __str__(self) -> str:
        return f"\n\t\tpage {self.id}: {self.content}"


class file_old:
    def __init__(self, name, page_table, col_cache) -> None:
        print(f"\t\tmaking file: {name}")
        self.attr = name
        self.map = {}
        self.page_map = {}
        self.pages = []
        self.pg_tbl = page_table
        self.col_cache = col_cache

    def get_map(self, id):
        if id not in self.map.keys():
            # id is not in the file yet
            print(f"id {id} is not in file map yet ")
            # return the best page for it to be on
            for page in self.pages:
                if self.pg_tbl.get_entry(page.id).valid:
                    print("page found is in buffer")
                    if not self.col_cache.get(
                        pg_tbl.get_entry(page.id).frame_num
                    ).full():
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
        self.page_map[_id] = len(self.pages) - 1
        return _id

    def get_page(self, _id) -> Page:
        # do some check bounds etc
        return self.pages[self.page_map[_id]]

    def __str__(self) -> str:
        return f"\n\t{self.attr} " + "".join([str(x) for x in self.pages])


class table_old:
    def __init__(self, name, PK, attrs, page_table, column_cache) -> None:
        print(f"make table: {name}")
        self.name = name
        self.PK = PK
        self.attrs = {}

        for attr in attrs:
            print(f"\tTABLE Making attr file: {attr}")
            self.attrs[attr] = File(attr, page_table, column_cache)

    def get_file(self, name):
        return self.attrs[name]

    def __str__(self):
        return f"{self.name} PK: {self.PK}" + "".join(
            [str(x) for x in self.attrs.values()]
        )


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


class ColBuffer:
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
                f"COL BUFFER failure to add {page.id} at {idx} with buffer len: {len(self.buffer)}"
            )
            return False

    def get(self, idx):
        print(f"COL BUFFER getting {idx} from {len(self.buffer)} size buffer")
        if idx in self.buffer.keys():
            return self.buffer[idx]
        print(f"COL BUFFER idx not in buffer: {self.buffer.keys()}")
        return None

    def to_row(self, table, c_id, record):
        #
        # create the record
        # return it to data manager or can send to directly to row buffer
        print(f"TO ROW table: {table} c_id: {c_id}")

        # row_cache.add(tuple_r)
        row_cache.add(table, record)
        # add the tuple to the row buffer

    def __str__(self) -> str:
        if len(self.buffer) == 0:
            print("COLUMN BUFFER EMPTY")
        return "BUFFER: " + "".join([str(x) for x in self.buffer.values()])


class RowBuffer:
    def __init__(self, size) -> None:
        self.buffer = {}
        self.max = size
        self.map = {}  # maps (table id, CoffeeIDs) to Row Buffer Pages

    def do_op(self, op):
        # data manager will send ops to buffer such as flushing
        # or sending to row buffer
        return

    def read(self, t_id, c_id):
        # read a coffee id from the row buffer
        if (t_id, c_id) not in self.map.keys():
            # c_id not in row buffer
            return None
        else:
            page_n = self.map[c_id]
            page = self.buffer[page_n]
            for tup in page.contents:
                if tup[0] == t_id and tup[1][0] == c_id:
                    return tup[1]

    def flush(self, t_id, c_id):
        key = (t_id, c_id)
        if key not in self.map.keys():
            # c_id not in row buffer
            return
        page_n = self.map[key]
        page = self.buffer[page_n]
        del self.map[key]
        for tup in page.contents:
            if tup[1][0] == key:
                # found the tuple
                t_id = tup[0]
                tup = tup[1]
                id_ = tup[1][0]
                insert(t_id, id_, tup)
                # for each attr get page num and insert tup to that page num

    def flush_all(self):
        for id_ in self.map.keys():
            self.flush(id_)
        self.map = {}
        self.buffer = {}

    def set(self, idx, page):
        # sets idx to page
        raise NotImplementedError

    def to_col(self, idx):
        # create the columns
        # return it to data manager or can send to directly to col buffer
        raise NotImplementedError

    def add(self, t_id, c_id, record):
        # add the record to the buffer or update
        # if not in the buffer add it to buffer
        if (t_id, c_id) not in self.map.keys():
            # c_id not in row buffer
            try:
                last = max(self.buffer.keys())
            except:
                self.buffer[0] = Page(0)
                self.map[t_id] = {}
                self.map[t_id][record[0]] = 0
                last = 0
            if self.buffer[last].full():
                # need to add a page
                if len(self.buffer.keys()) + 1 > self.max:
                    # cant add a page
                    self.flush_all()
                    self.buffer[0] = Page(0)
                    self.buffer[0].add_tuple(record)
                    self.map[t_id][record[0]] = 0
                else:
                    idx = len(self.buffer.keys())
                    self.buffer[idx] = Page(idx)
                    self.buffer[idx].add_tuple((t_id, record))
            else:
                # can add to last page
                self.buffer[last].add_tuple((t_id, record))
        else:
            page_n = self.map[c_id]
            page = self.buffer[page_n]
            for tup in page.contents:
                if tup[0] == t_id and tup[1][0] == c_id:
                    record_ = tup[1]


class DataManager(Component):
    def __init__(
        self, catalog_manager: CatalogManager, tables: dict, buff_size: int
    ) -> None:
        super().__init__(LogType.DATA_MANAGER)
        self.catalog_manager = catalog_manager
        self.tables = tables
        self.recovery_log = {}
        self.pg_tbl = PageTable()
        self.col_cache = ColBuffer(buff_size)
        self.row_cache = RowBuffer(buff_size)
        self.lru_arr = []
        self.log("data manager component intialized message")

    def get_table(self, table_key: str) -> Table:
        return self.tables.get(table_key)

    def set_table(self, table_key: str, table: Table) -> None:
        self.tables[table_key] = table

    def execute_op(self, op):
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

    def lru(self):
        # need a way to know that there is an invalid mapping and can therefore be overwritten
        print(f"LRU: len: {len(self.col_cache.buffer)} max: {self.col_cache.max}")
        if len(self.col_cache.free) > 0:
            # empty slot!
            # fill it.
            idx = self.col_cache.free.pop(0)
            print(f"LRU: empty slot in buffer return {idx}")
            return idx
        try:
            evict = self.lru_arr.pop(0)
            evict = self.pg_tbl.get_entry(evict)
            self.col_cache.flush(evict)
            print(f"LRU: chose {evict} to evict")
            return evict
        except:
            evict = random.randrange(len(self.col_cache.buffer))
            self.col_cache.flush(evict)
            print(f"LRU: chose {evict} to evict")
            return evict

    # G table val: Counts the number of coffees which have val as intensity in table. If table does not exist, the group-by-count is aborted
    def g_op(self, table_id: str, val):
        # return from aggregate thing on the file
        raise NotImplementedError

    # M table val: Retrieve the coffee name(s) for all record(s) with countryOfOrigin=val in table. If table does not exist, the read is aborted.
    def op_m(self, table_id: str, val):
        raise NotImplementedError

    # T table: Retrieve all the record(s) from table. If table does not exist, the read is aborted
    def table_read(self, table_id: str):
        if table_id not in self.catalog_manager.catalogs.keys():

            # table does not exist
            return 0  # no op
        table = self.get_table(table_id)

        # how to get all the CoffeeIDs?
        list_ = []
        attr1 = list(table.attrs.keys())[0]
        file = table.attrs[attr1]
        for page_num in file.page_map.keys():
            entry_rec = self.pg_tbl.get_entry(page_num)
            if entry_rec is None or entry_rec.valid == False:  # not in the buffer
                # evict a page (LRU)
                print("TABLE READ entry_rec is None\nCall LRU")
                frame_num = self.lru()
                print(f"TABLE READ evicted {frame_num}")
                # update page table w that page num and frame num
                print("TABLE READ create page table entry")
                entry = PageTableEntry(frame_num)
                print(f"TABLE READ set page table entry {page_num} {entry} ")
                self.pg_tbl.set_entry(page_num, entry)
                entry.valid = True
                if page_num in self.lru_arr:
                    self.lru_arr.remove(page_num)
                self.lru_arr.append(page_num)
                # put page in buffer at mapping
                # page = disc_mngr.get(File, page_num)
                print(f"TABLE READ set page from file page num: {page_num}")
                page = file.get_page(page_num)
                print(f"TABLE READ set cache value {frame_num} {page_num}")
                self.col_cache.set(frame_num, page)
                # overwrite old page this is like pointer stuff. we want the deep copy version thats unchanged until we flush
                page = self.col_cache.get(frame_num)
                print("TABLE READ col cache after set: ", self.col_cache)
                if page is None:
                    quit("TABLE READ 1: page is none")
            else:
                print(f"TABLE READget page from cache {entry_rec.frame_num}")
                page = self.col_cache.get(entry_rec.frame_num)
                entry_rec.valid = True
                if page_num in self.lru_arr:
                    self.lru_arr.remove(page_num)
                self.lru_arr.append(page_num)
                if page is None:
                    quit("TABLE READ 2: page is none")
            # Now page is in buffer
            for tup in page.content:
                list_.append(tup[0])

        str_r = ""
        for id in list_:
            str_r += f"\n{self.read(table_id, id)}"
        return str_r

    # R table val: Retrieve record(s) with coffeeID = val. If table does not exist, the read is aborted.
    def read(self, table_id: str, ID):
        if table_id not in self.catalog_manager.catalogs.keys():

            # table does not exist
            return 0  # no op
        table = self.get_table(table_id)
        if table.blm_fltr[ID] == 0:  # if tuple is Null
            return 0  # no op
        # for attr in attrs
        frames = []
        for attr in ["CoffeeName", "Intensity", "CountryOfOrigin"]:
            file = table.attrs[attr]
            page_num = file.get_map(ID)
            if page_num is None:
                print("READ page num is none: id not in table")
                return 0
            entry_rec = self.pg_tbl.get_entry(page_num)
            if entry_rec is None or entry_rec.valid == False:  # page not in buffer
                # evict a page (LRU)
                print("READ entry_rec is None\nCall LRU")
                frame_num = self.lru()
                print(f"READ evicted {frame_num}")
                # update page table w that page num and frame num
                print("READ create page table entry")
                entry = PageTableEntry(frame_num)
                print(f"READ set page table entry {page_num} {entry} ")
                self.pg_tbl.set_entry(page_num, entry)
                entry.valid = True
                if page_num in self.lru_arr:
                    self.lru_arr.remove(page_num)
                self.lru_arr.append(page_num)
                # put page in buffer at mapping
                # page = disc_mngr.get(File, page_num)
                print(f"READ set page from file page num: {page_num}")
                page = file.get_page(page_num)
                print(f"READ set cache value {frame_num} {page_num}")
                self.col_cache.set(frame_num, page)
                # overwrite old page this is like pointer stuff. we want the deep copy version thats unchanged until we flush
                page = self.col_cache.get(frame_num)
                print("READ col cache after set: ", self.col_cache)
                if page is None:
                    quit("READ 1: page is none")
            else:
                print(f"READget page from cache {entry_rec.frame_num}")
                page = self.col_cache.get(entry_rec.frame_num)
                entry_rec.valid = True
                if page_num in self.lru_arr:
                    self.lru_arr.remove(page_num)
                self.lru_arr.append(page_num)
                if page is None:
                    quit("READ 2: page is none")

            self.col_cache.to_row(table_id, ID, frames)
            frames.append(entry_rec.frame_num)
        print(frames)

        # call to_row() and append the tuple into row buffer
        return self.row_cache.read(table_id, ID)
        # return the tuple

    # U table (ID, val): Update the intensity of the coffeeID=ID to be val. If table does not exist, it is created
    def update(self, table_id, ID, val):
        global num_pages
        # raise NotImplementedError
        print("UPDATE UPDATEing a tuple")

        if val == None or blm_flt[ID] == 0:  # if tuple is Null
            return 0  # no op

        # check catalog for table

        # check catalog for table
        if table_id not in self.catalog_manager.catalogs.keys():
            print("INSERT Create table")
            # if doesnt exist, Create
            self.catalog_manager.insert_catalog(table_id)
            # TODO should be connected to catalog manager/schema

            table_new = Table(
                table_id,
                "coffee_id",
                ["coffee_name", "intensity", "country_of_origin"],
                self.pg_tbl,
                self.col_cache,
            )
            self.set_table(table_id, table_new)

        table = self.get_table(table_id)
        attr_tup = (ID, val)
        print("\n")

        print(f"UPDATE on 'Intensity' with tuple: {attr_tup}")
        # use access method to get to page num for UPDATEing
        c_id = attr_tup[0]
        # val = attr_tup[1]
        file = table.attrs["Intensity"]
        print(f"UPDATE c_id: {c_id}")
        page_num = file.get_map(c_id)
        if page_num is None:
            print("UPDATE page num is none add a page")
            # make a page
            num_pages += 1
            page_num = file.add_page(num_pages)
            print(f"UPDATE update the map c_id: {c_id} to page_num: {page_num}")
            file.update_map(c_id, page_num)

        # check page table for page num
        print(f"UPDATE get entry from page table for page_num {page_num}")
        entry_rec = self.pg_tbl.get_entry(page_num)
        if entry_rec is None or entry_rec.valid == False:  # not in the buffer
            # evict a page (LRU)
            print("UPDATE entry_rec is None\nCall LRU")
            frame_num = self.lru()
            print(f"UPDATE evicted {frame_num}")
            # update page table w that page num and frame num
            print("UPDATE create page table entry")
            entry = PageTableEntry(frame_num)
            print(f"UPDATE set page table entry {page_num} {entry} ")
            self.pg_tbl.set_entry(page_num, entry)
            entry.valid = True
            entry.dirty = True
            if page_num in self.lru_arr:
                self.lru_arr.remove(page_num)
            self.lru_arr.append(page_num)
            # put page in buffer at mapping
            # page = disc_mngr.get(File, page_num)
            print(f"UPDATE set page from file page num: {page_num}")
            page = file.get_page(page_num)
            print(f"UPDATE set cache value {frame_num} {page_num}")
            self.col_cache.set(frame_num, page)
            # overwrite old page this is like pointer stuff. we want the deep copy version thats unchanged until we flush
            page = self.col_cache.get(frame_num)
            print("UPDATE col cache after set: ", self.col_cache)
            if page is None:
                quit("UPDATE 1: page is none")
        else:
            print(f"UPDATEget page from cache {entry_rec.frame_num}")
            page = self.col_cache.get(entry_rec.frame_num)
            entry_rec.dirty = True
            entry_rec.valid = True
            if page_num in self.lru_arr:
                self.lru_arr.remove(page_num)
            self.lru_arr.append(page_num)
            if page is None:
                quit("UPDATE 2: page is none")
        # Now page is in buffer
        # use page map to find correct position for tuple in the page

        # UPDATE the tuple
        print("UPDATE UPDATE tuple to page")
        page.add_tuple(attr_tup)
        print("UPDATE col cache after tuple UPDATE: ", self.col_cache)
        # update the map
        print(f"UPDATE update file map {c_id} {page_num}")
        file.update_map(c_id, page_num)
        # update the access method if need be
        # update the bloom filter
        blm_flt[c_id] = 1
        print("\n")
        print(self.col_cache)
        return True

    # I table (t): Insert the new record t = (coffeeID, coffeeName, intensity, countryOfOrigin) into table. If table does not exist, this operation should create that table.
    def insert(self, table_id, ID, tuple_i):
        global num_pages
        print("INSERT inserting a tuple")
        # check bloom filter for ID
        # if blm_flt[ID] == 1: # if it is there
        #     print("ID present in data (probably) updating...")
        #     return update(table, ID, tuple_i) # Call Update
        # else: # if not
        #     print("ID not present in data")

        # check catalog for table
        if table_id not in self.catalog_manager.catalogs.keys():
            print("INSERT Create table")
            # if doesnt exist, Create
            self.catalog_manager.insert_catalog(table_id)
            # TODO should be connected to catalog manager/schema

            table_new = Table(
                table_id,
                "coffee_id",
                ["coffee_name", "intensity", "country_of_origin"],
                self.pg_tbl,
                self.col_cache,
            )
            self.set_table(table_id, table_new)

        table = self.get_table(table_id)

        if tuple_i == None and table.blm_fltr[ID] == 0:  # if tuple is Null
            return 0  # no op
        # turn record into tuples
        print("INSERT Create tuples from record")
        tuples = (
            ("coffee_name", (ID, tuple_i[0])),
            ("intensity", (ID, tuple_i[1])),
            ("country_of_origin", (ID, tuple_i[2])),
        )

        # for each tuple

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
                num_pages += 1
                page_num = file.add_page(num_pages)
                print(f"INSERT update the map c_id: {c_id} to page_num: {page_num}")
                file.update_map(c_id, page_num)

            # check page table for page num
            print(f"INSERT get entry from page table for page_num {page_num}")
            entry_rec = self.pg_tbl.get_entry(page_num)
            if entry_rec is None or entry_rec.valid == False:  # not in the buffer
                # evict a page (LRU)
                print("INSERT entry_rec is None\nCall LRU")
                frame_num = self.lru()
                print(f"INSERT evicted {frame_num}")
                # update page table w that page num and frame num
                print("INSERT create page table entry")
                entry = PageTableEntry(frame_num)
                print(f"INSERT set page table entry {page_num} {entry} ")
                self.pg_tbl.set_entry(page_num, entry)
                entry.valid = True
                entry.dirty = True
                if page_num in self.lru_arr:
                    self.lru_arr.remove(page_num)
                self.lru_arr.append(page_num)
                # put page in buffer at mapping
                # page = disc_mngr.get(File, page_num)
                print(f"INSERT set page from file page num: {page_num}")
                page = file.get_page(page_num)
                print(f"INSERT set cache value {frame_num} {page_num}")
                self.col_cache.set(frame_num, page)
                # overwrite old page this is like pointer stuff. we want the deep copy version thats unchanged until we flush
                page = self.col_cache.get(frame_num)
                print("INSERT col cache after set: ", self.col_cache)
                if page is None:
                    quit("INSERT 1: page is none")
            else:
                print(f"INSERTget page from cache {entry_rec.frame_num}")
                page = self.fcol_cache.get(entry_rec.frame_num)
                entry_rec.dirty = True
                entry_rec.valid = True
                if page_num in self.lru_arr:
                    self.lru_arr.remove(page_num)
                self.lru_arr.append(page_num)

                if page is None:
                    quit("INSERT 2: page is none")
            # Now page is in buffer
            # use page map to find correct position for tuple in the page

            # insert the tuple
            print("INSERT insert tuple to page")
            page.add_tuple(attr_tup)

            print("INSERT col cache after tuple insert: ", self.col_cache)
            # update the map
            print(f"INSERT update file map {c_id} {page_num}")
            file.update_map(c_id, page_num)
            # update the access method if need be
            # update the bloom filter
            blm_flt[c_id] = 1
            print("\n")
        print(self.col_cache)
        return True


dt_mngr = DataManager(4)

dt_mngr.insert("starbucks", 0, ("latte", 5, "USA"))
print(dt_mngr.get_table("starbucks"))  # table print
print(dt_mngr.col_cache)

dt_mngr.insert("starbucks", 0, ("mochiato", 10, "France"))
print(dt_mngr.get_table("starbucks"))  # table print


dt_mngr.insert("starbucks", 1, ("nitro", 12, "USA"))
print(dt_mngr.get_table("starbucks"))  # table print

dt_mngr.col_cache.flush(0)
print(dt_mngr.get_table("starbucks"))  # table print
print(dt_mngr.col_cache)

dt_mngr.update("starbucks", 1, 3)
print(dt_mngr.get_table("starbucks"))  # table print
print(dt_mngr.col_cache)

print("\nSTART READ")
print(dt_mngr.read("starbucks", 1))

dt_mngr.insert("starbucks", 2, ("latte", 5, "ITALLIIIAAA"))
print(dt_mngr.get_table("starbucks"))  # table print
print(dt_mngr.col_cache)

print("START TABLE READ")
print(dt_mngr.table_read("starbucks"))


dt_mngr.col_cache.full_flush()
print(dt_mngr.get_table("starbucks"))  # table print
print(dt_mngr.col_cache)
