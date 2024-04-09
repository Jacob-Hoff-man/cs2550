from enum import Enum
from Logger import LoggerSingleton as l, LogType

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
    def __init__(self, table_key, attrs) -> None:
        self.table_key = table_key
        self.attributes = {}
        for attribute in self.attributes:
            print(f"\tMaking attr file: {attribute}")
            self.attributes[attribute] = File(attribute)

    def get_file(self, name):
        return self.attributes[name]
