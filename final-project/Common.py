from enum import Enum

class OrgType(Enum):
    HASH = 0
    ORDERED = 1
    HEAP = 2

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
