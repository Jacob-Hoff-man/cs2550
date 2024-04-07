from enum import Enum

class OrgType(Enum):
    HASH = 0
    ORDERED = 1
    HEAP = 2

class ViewType(Enum):
    COUNT = 0

class AuxType(Enum):
    B_PLUS_TREE = 0
    R_TREE = 1
    ISAM = 2
    CLUSTERED = 3

class FilterType(Enum):
    BLOOM = 0

class RecOrgType(Enum):
    ROW = 0
    COLUMN = 1

class RecordCoffee():
    def __init__(self, coffee_id, coffee_name, intensity, country_of_origin) -> None:
        self.coffee_id = coffee_id
        self.coffee_name = coffee_name
        self.intensity = intensity
        self.country_of_origin = country_of_origin

class Component():
    pass

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

