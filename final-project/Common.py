from enum import Enum
from Logger import LoggerSingleton as l, LogType
import copy


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


class RecordCoffee():
    def __init__(self, coffee_id, coffee_name, intensity, country_of_origin) -> None:
        self.coffee_id = coffee_id
        self.coffee_name = coffee_name
        self.intensity = intensity
        self.country_of_origin = country_of_origin


class Component():
    def __init__(self, log_type: LogType) -> None:
        self.log_type = log_type

    def log(self, message):
        l.log(self.log_type, message)


class AccessMethod():
    pass


class Auxiliary(AccessMethod):
    def set(self, key, page_key):
        self.pages[key] = page_key

    def get(self, key):
        return self.pages.get(key)


class Filter(AccessMethod):
    pass


class Aggregate(AccessMethod):
    def set(self, key, aggregate_value):
        self.values[key] = aggregate_value

    def get(self, key):
        return self.values.get(key)


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
                    if not self.col_cache.get(self.pg_tbl.get_entry(page.id).frame_num).full():
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

    def add_page(self, id) -> int:
        # global num_pages
        # print(f"FILE num pages is {num_pages}")
        # _id = num_pages
        # num_pages += 1

        new_page = Page(id)
        self.pages.append(new_page)
        self.page_map[id] = len(self.pages)-1
        return id

    def get_page(self, _id) -> Page:
        # do some check bounds etc
        return self.pages[self.page_map[_id]]

    def __str__(self) -> str:
        return f"\n\t{self.attr} " + "".join([str(x) for x in self.pages])


class Table():
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
        return f"{self.name} PK: {self.PK}" + "".join([str(x) for x in self.attrs.values()])
