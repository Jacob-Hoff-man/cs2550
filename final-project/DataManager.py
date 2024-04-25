import copy
import random

from CatalogManager import CatalogManager
from Common import AuxiliaryType, Component, File, Page, Record, Table

# from Common import Component
from Logger import LogType

num_pages = 0


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
            print(self.map.keys())
            return self.map[page_num]

    def set_entry(self, page_num, page_table_entry):
        self.map[page_num] = page_table_entry


class DataManager(Component):
    def __init__(
        self, catalog_manager: CatalogManager, tables: dict, buff_size: int
    ) -> None:
        super().__init__(LogType.DATA_MANAGER)
        self.catalog_manager = catalog_manager
        self.tables = tables
        self.recovery_log = {}
        self.pg_tbl = PageTable()
        self.col_cache = ColBuffer(self, buff_size)
        self.row_cache = RowBuffer(self, buff_size)
        self.lru_arr = []
        self.log("data manager component intialized message")

    def write_out(self, page: Page):

        pg_id = page.id
        print(f"writing out page w id {pg_id}")

        for table in self.tables.values():
            for file in table.attrs.values():
                i = 0
                for page_ in file.pages:
                    if page_.id == pg_id:
                        print("Found the page on disc")
                        file.pages[i] = copy.deepcopy(page)
                        return
                    i += 1
        print("page wasnt found on disk")

    def get_table(self, table_key: str) -> Table:
        return self.tables.get(table_key)

    def set_table(self, table_key: str, table: Table) -> None:
        self.tables[table_key] = table

    def lru(self):
        # need a way to know that there is an invalid mapping and can therefore be overwritten
        print(
            f"LRU: len: {len(self.col_cache.buffer)} max: {self.col_cache.max}")
        if len(self.col_cache.free) > 0:
            # empty slot!
            # fill it.
            idx = self.col_cache.free.pop(0)
            print(f"LRU: empty slot in buffer return {idx}")
            return idx

        evict = self.lru_arr.pop(0)
        print(f"LRU: chose {evict} to evict")
        self.col_cache.flush(evict)

        return evict
        # except Exception as e:
        #     print(f"Exception: {e}")
        #     evict = random.randrange(len(self.col_cache.buffer))
        #     self.col_cache.flush(evict)
        #     print(f"LRU: chose {evict} to evict")
        #     return evict

    def get_page_to_buffer(self, page_num: int, file: File) -> Page:
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
            entry.valid = True
            entry.dirty = True
            print(f"INSERT set page table entry {page_num} {entry} ")

            self.pg_tbl.set_entry(page_num, entry)

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
            print(f"INSERT get page from cache {entry_rec.frame_num}")
            page = self.col_cache.get(entry_rec.frame_num)
            entry_rec.dirty = True
            entry_rec.valid = True
            if page_num in self.lru_arr:
                self.lru_arr.remove(page_num)
            self.lru_arr.append(page_num)

            if page is None:
                quit("INSERT 2: page is none")
        return page

    # G table val: Counts the number of coffees which have val as intensity in table. If table does not exist, the group-by-count is aborted

    def g_op(self, t_id: str, val):
        # return from aggregate thing on the file
        raise NotImplementedError

    # M table val: Retrieve the coffee name(s) for all record(s) with countryOfOrigin=val in table. If table does not exist, the read is aborted.
    def op_m(self, t_id: str, val):
        raise NotImplementedError

    def read_name_tuple(self, t_id, c_id) -> tuple | None:
        global num_pages
        table = self.get_table(t_id)

        file = table.attrs["intensity"]  # this is a File()
        assert isinstance(file, File)
        acc_m = self.catalog_manager.get_auxiliary(t_id, "intensity")
        page_num = acc_m.get(c_id)
        if page_num is None:
            return None
        else:
            page = self.get_page_to_buffer(page_num, file)

            _tuple = page.get_tuple(c_id)
            if _tuple is None:
                return None
            return _tuple

    def read_intensity_tuple(self, t_id, c_id) -> tuple | None:
        global num_pages
        table = self.get_table(t_id)

        file = table.attrs["intensity"]  # this is a File()
        assert isinstance(file, File)
        acc_m = self.catalog_manager.get_auxiliary(t_id, "intensity")
        page_num = acc_m.get(c_id)
        if page_num is None:
            return None
        else:
            page = self.get_page_to_buffer(page_num, file)

            _tuple = page.get_tuple(c_id)
            if _tuple is None:
                return None
            return _tuple

    def read_coo_tuple(self, t_id, c_id) -> tuple | None:
        global num_pages
        table = self.get_table(t_id)

        file = table.attrs["country_of_origin"]  # this is a File()
        assert isinstance(file, File)
        # acc_m = self.catalog_manager.get_auxiliary(t_id, "country_of_origin")
        for page_num in file.page_map.keys():
            page = self.get_page_to_buffer(page_num, file)

            _tuple = page.get_tuple(c_id)
            if _tuple is not None:
                # tuple found
                return _tuple
        return None

    # T table: Retrieve all the record(s) from table. If table does not exist, the read is aborted
    def table_read(self, t_id: str) -> str:
        if t_id not in self.catalog_manager.catalogs.keys():
            # table does not exist
            return 0  # no op

        table = self.get_table(t_id)
        file = table.attrs["coffee_name"]

        list_ = []
        for page_num in file.page_map.keys():
            page = self.get_page_to_buffer(page_num, file)
            for c_id, _ in page.content:
                list_.append(c_id)

        str_r = ""
        for id in list_:
            str_r += f"\n{self.read(t_id, id)}"
        return str_r

    # R table val: Retrieve record(s) with coffeeID = val. If table does not exist, the read is aborted.
    def read(self, t_id: str, c_id: int) -> str:
        if t_id not in self.catalog_manager.catalogs.keys():
            # table does not exist
            return 0  # no op

        # table = self.get_table(t_id)
        blm_fltr = self.catalog_manager.get_filter(t_id, "coffee_id")
        if c_id in blm_fltr:  # if tuple is Null

            print(f"READ c_id: {c_id} IS NOT IN THE TABLE")
            return 0  # no op

        _, name_val = self.read_name_tuple(t_id, c_id)
        self.col_cache.to_row(t_id, c_id, name_val)

        _, intensity_val = self.read_intensity_tuple(t_id, c_id)
        self.col_cache.to_row(t_id, c_id, intensity_val)

        _, coo_val = self.read_coo_tuple(t_id, c_id)
        self.col_cache.to_row(t_id, c_id, coo_val)

        # call to_row() and append the tuple into row buffer
        return self.row_cache.read(t_id, c_id)
        # return the tuple

    # U table (c_id, val): Update the intensity of the coffeeID=ID to be val. If table does not exist, it is created
    def update(self, t_id, c_id, val) -> bool:
        global num_pages

        blm_fltr = self.catalog_manager.get_filter(t_id, "coffee_id")
        # raise NotImplementedError
        print("UPDATE UPDATEing a tuple")
        if val == None or c_id not in blm_fltr:  # if tuple is Null
            return 0  # no op
        # check catalog for table
        # check catalog for table
        if t_id not in self.catalog_manager.catalogs.keys():
            print("UPDATE Create table")
            # if doesnt exist, Create
            self.catalog_manager.insert_catalog(t_id)
            # TODO should be connected to catalog manager/schema

            table_new = Table(
                t_id,
                "coffee_id",
                ["coffee_name", "intensity", "country_of_origin"],
                self.pg_tbl,
                self.col_cache,
            )
            self.set_table(t_id, copy.deepcopy(table_new))

        self.insert_intensity_tuple(t_id, c_id, val)
        blm_fltr.add(c_id)

        print("\n")
        return True

    def insert_name_tuple(self, t_id, c_id, val):
        global num_pages
        print("INSERTING  NAME  TUPLE")
        table = self.get_table(t_id)
        file = table.attrs["coffee_name"]
        acc_m = self.catalog_manager.get_auxiliary(t_id, "coffee_name")

        rtrn = acc_m.get(c_id)
        if rtrn is None:
            print("Adding a  new  page")
            # return was none
            # create a new page where c_id will be the anchor
            # then recreate the acc_m
            # make a page
            page_num = file.add_page(num_pages)
            num_pages += 1
            page_anchor = c_id
            index = acc_m.get_index()
            recreate_page_numbers = [(page_anchor, page_num)]
            for index_anchor in index:
                index_page_number = acc_m.page_numbers.get(index_anchor)
                recreate_page_numbers.append((index_anchor, index_page_number))
            acc_m.recreate(recreate_page_numbers)
        else:
            page_anchor, page_num = rtrn

        page = self.get_page_to_buffer(page_num, file)
        # insert the tuple
        print("INSERT insert tuple to page")
        page.add_tuple((c_id, val))
        anchor, _ = page.get_anchor()
        acc_m.set(anchor, page.id)
        print("INSERTED  NAME  TUPLE\n\n")
        return

    def insert_intensity_tuple(self, t_id, c_id, val):
        global num_pages
        print("INSERTING  INTENSITY  TUPLE")
        table = self.get_table(t_id)
        file = table.attrs["intensity"]
        acc_m = self.catalog_manager.get_auxiliary(t_id, "intensity")

        rtrn = acc_m.get(c_id)
        if rtrn == None:
            page_num = file.add_page(num_pages)
            num_pages += 1
            page_anchor = c_id
            index = acc_m.get_index()
            recreate_page_numbers = [(page_anchor, page_num)]
            for index_anchor in index:
                index_page_number = acc_m.page_numbers.get(index_anchor)
                recreate_page_numbers.append((index_anchor, index_page_number))
            acc_m.recreate(recreate_page_numbers)
        else:
            page_anchor, page_num = rtrn
        page = self.get_page_to_buffer(page_num, file)
        # use page map to find correct position for tuple in the page

        # insert the tuple
        print("INSERT insert tuple to page")
        agg = self.catalog_manager.get_aggregate(t_id, "intensity")

        rtrn = page.get_tuple(c_id)
        if rtrn is None:
            old_val = None
        else:
            _, old_val = rtrn

        if old_val is not None:
            agg.decrement(old_val)

        page.add_tuple((c_id, val))
        agg.increment(val)
        anchor, _ = page.get_anchor()
        acc_m.set(anchor, page.id)
        print("INSERTED  INTENSITY  TUPLE\n\n")
        return

    def update_coo_tuple(self, t_id, c_id, val):
        table = self.get_table(t_id)
        file = table.attrs["country_of_origin"]
        for page_num in file.page_map.keys():
            page = self.get_page_to_buffer(page_num, file)
            # insert the tuple
            print("INSERT insert tuple to page")
            rtrn = page.add_tuple((c_id, val))
            print(f"attempted  to  add {c_id} to page: {page.id}")
            if rtrn == 0:
                print("INSERT tuple inserted")
                # sucessful add to the page
                return 0
        print("tuple not found")
        return 1

    def insert_coo_tuple(self, t_id, c_id, val):
        global num_pages
        print("INSERTING  COO  TUPLE")
        table = self.get_table(t_id)
        file = table.attrs["country_of_origin"]
        acc_m = self.catalog_manager.get_auxiliary(t_id, "country_of_origin")

        page_nums = acc_m.get(val)  # this does work if this is an update.
        # check bloom filter
        blm_fltr = self.catalog_manager.get_filter(t_id, "coffee_id")
        if c_id in blm_fltr:
            # need to do an update
            if self.update_coo_tuple(t_id, c_id, val) == 0:
                return 0
        if page_nums is None:
            print("INSERT COO PAGE NUMS WAS  NULL")
            # return was none
            # value not in the index yet
            # then recreate the acc_m
            # make a page

            page_num = file.add_page(num_pages)
            num_pages += 1
            page_anchor = val
            acc_m.set(page_anchor, page_num)
            page_nums = acc_m.get(val)
        print(f"search pages in list:  {page_nums}")
        for page_num in page_nums:
            page = self.get_page_to_buffer(page_num, file)
            # insert the tuple
            print("INSERT insert tuple to page")
            rtrn = page.add_tuple((c_id, val))
            print(f"attempted  to  add {c_id} to page: {page.id}")
            if rtrn == 0:
                print("INSERT tuple inserted")
                # sucessful add to the page
                return 0
            # else  page was full
        # need to add to a diff  page
        print("need to add a page")
        page_num = file.add_page(num_pages)
        num_pages += 1
        page_anchor = val
        acc_m.set(page_anchor, page_num)
        page_nums = acc_m.get(val)

        page = self.get_page_to_buffer(page_num, file)
        print("INSERT insert tuple to page")
        if page.add_tuple((c_id, val)) == 0:
            # sucessful add to the page
            return 0
        # how to link  new one to  this  anmchor?
        # since we are clustering we need  to check  if the tuple can fit on this page
        # if it doesnt, we need to make a  new page.
        print("INSERTED COO TUPLE\n\n")
        return 0

    # I table (t): Insert the new record t = (coffeec_id, coffeeName, intensity, countryOfOrigin) into table. If table does not exist, this operation should create that table.
    def insert(self, t_id, c_id, tuple_i) -> bool:
        global num_pages
        print("INSERT inserting a tuple")

        # check catalog for table
        if t_id not in self.catalog_manager.catalogs.keys():
            print("INSERT Create table")
            # if doesnt exist, Create
            self.catalog_manager.insert_catalog(t_id)
            # TODO should be connected to catalog manager/schema

            table_new = Table(
                t_id,
                "coffee_id",
                ["coffee_name", "intensity", "country_of_origin"],
                self.pg_tbl,
                self.col_cache,
            )
            self.set_table(t_id, copy.deepcopy(table_new))

        # table = self.get_table(t_id)
        blm_fltr = self.catalog_manager.get_filter(t_id, "coffee_id")
        if tuple_i == None and c_id in blm_fltr:  # if tuple is Null
            return True  # no op
        # turn record into tuples
        print("INSERT Create tuples from record")

        self.insert_name_tuple(t_id, c_id, tuple_i[0])
        self.insert_intensity_tuple(t_id, c_id, tuple_i[1])
        self.insert_coo_tuple(t_id, c_id, tuple_i[2])

        # update the bloom filter
        blm_fltr.add(c_id)
        print("\n")

        return True


class ColBuffer:
    def __init__(self, dt_mngr: DataManager, size: int) -> None:
        self.buffer = {}
        self.max = size  # max index in buffer (used by page table)
        self.free = [x for x in range(self.max)]
        self.dt_mngr = dt_mngr

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
        pg_entry = self.dt_mngr.pg_tbl.get_entry(page.id)
        print(f"got entry from page table: {pg_entry}")
        pg_entry.valid = False
        print("make mapping false")
        if pg_entry.dirty:
            print("page is dirty, write it out")
            self.dt_mngr.write_out(page)
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
        except Exception as e:
            print(f"Exception: {e}")
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

    def to_row(self, t_id, c_id, record):
        #
        # create the record
        # return it to data manager or can send to directly to row buffer
        print(f"TO ROW table: {t_id} c_id: {c_id}")

        # row_cache.add(tuple_r)
        self.dt_mngr.row_cache.add(t_id, c_id, record)

    def __str__(self) -> str:
        # if len(self.buffer) == 0:
        print("COLUMN BUFFER EMPTY")
        return "BUFFER: " + "".join([str(x) for x in self.buffer.values()])


class RowBuffer:
    def __init__(self, dt_mngr: DataManager, size) -> None:
        self.buffer = {}
        self.max = size
        self.map = {}  # maps (table id, CoffeeIDs) to Row Buffer Pages
        self.dt_mngr = dt_mngr

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
                self.dt_mngr.insert(t_id, id_, tup)
                # for each attr get page num and insert tup to that page num

    def flush_all(self):
        for tu_ in self.map.keys():
            self.flush(tu_[0], tu_[1])
        self.map = {}
        self.buffer = {}

    def set(self, idx, page):
        # sets idx to page
        raise NotImplementedError

    def to_col(self, idx):
        # create the columns
        # return it to data manager or can send to directly to col buffer
        raise NotImplementedError

    def add(self, t_id: str, c_id: int, record: Record):
        # add the record to the buffer or update
        # if not in the buffer add it to buffer
        if (t_id, c_id) not in self.map.keys():
            # c_id not in row buffer
            try:
                last = max(self.buffer.keys())
            except Exception as e:
                print(f"Exception: {e}")
                self.buffer[0] = Page(0)
                self.map[t_id] = {}
                self.map[t_id][record.coffee_id] = 0
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
            # self.coffee_id = coffee_id
            # self.coffee_name = coffee_name
            # self.intensity = intensity
            # self.country_of_origin = country_of_origin
            page_n = self.map[c_id]
            page = self.buffer[page_n]
            for tup in page.contents:
                if tup[0] == t_id and tup[1][0] == c_id:
                    record_ = tup[1]
                    # if record_.coffeeID is None:
                    #     record_.coffeeID = record.coffeeID
                    if record_.coffee_name is None:
                        record_.coffee_name = record.coffee_name
                    if record_.intensity is None:
                        record_.intensity = record.intensity
                    if record_.country_of_origin is None:
                        record_.country_of_origin = record.country_of_origin
