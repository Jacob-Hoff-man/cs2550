import json
from Common import Component, FilterType, AuxiliaryType, AggregateType, Auxiliary, Filter
from Logger import LogType
from auxilaries.ClusteredIndex import ClusteredIndex
from auxilaries.BPlusTree import BPlusTree
from filters.BloomFilter import BloomFilter
from aggregates.Count import Count

class Catalog():
    def __init__(self) -> None:
        self.auxilaries = {}
        self.filters = {}
        self.aggregates = {}

class CatalogManager(Component):
    def get_auxiliary(self, table_key, column_key) -> Auxiliary:
        return self.catalogs.get(table_key).auxilaries.get(column_key)

    def get_filter(self, table_key, column_key) -> Filter:
        return self.catalogs.get(table_key).filters.get(column_key)

    def get_aggregate(self, table_key, column_key):
        return self.catalogs.get(table_key).aggregates.get(column_key)

    def set_auxiliary(self, table_key, column_key, auxiliary):
        self.catalogs[table_key].auxilaries[column_key] = auxiliary

    def set_filter(self, table_key, column_key, filter):
        self.catalogs[table_key].filters[column_key] = filter

    def set_aggregate(self, table_key, column_key, aggregate):
        self.catalogs[table_key].aggregates[column_key] = aggregate

    def insert_catalog(self, table_key):
        def get_catalog_auxiliary(auxiliary: AuxiliaryType):
            match auxiliary.value:
                case AuxiliaryType.CLUSTERED.value:
                    return ClusteredIndex()
                case AuxiliaryType.PRIMARY.value:
                    # TODO
                    return None
                case AuxiliaryType.R_TREE.value:
                    # TODO
                    return None
                case AuxiliaryType.B_PLUS_TREE.value:
                    # TODO: fix issue with BPlusTree().set throwing error
                    # return BPlusTree()
                    return None
                case _:
                    return None

        def get_catalog_filter(filter: FilterType):
            match filter.value:
                case FilterType.BLOOM.value:
                    return BloomFilter()
                case _:
                   return None

        def get_catalog_aggregate(aggregate: AggregateType):
            match aggregate.value:
                case AggregateType.COUNT.value:
                    return Count()
                case _:
                    return None

        catalog = Catalog()
        for column_name, column_definition in self.schema.items():
            access_methods = column_definition.get('access_methods')
            if access_methods.get('filter') != None:
                column_filter = get_catalog_filter(FilterType(access_methods.get('filter')))
                catalog.filters[column_name] = column_filter
            if access_methods.get('auxiliary') != None:
                column_auxiliary = get_catalog_auxiliary(AuxiliaryType(access_methods.get('auxiliary')))
                catalog.auxilaries[column_name] = column_auxiliary
            if access_methods.get('aggregate') != None:
                column_aggregate = get_catalog_aggregate(AggregateType(access_methods.get('aggregate')))
                catalog.aggregates[column_name] = column_aggregate
        self.catalogs[table_key] = catalog

    def delete_catalog(self, table_key):
        del self.catalogs[table_key]

    def read_schema_file(self, file_name):
        f = open(file_name)
        data = json.load(f)
        f.close()
        return data

    def get_column_definition(self, column_name):
        return self.schema[column_name]

    def __init__(self, file_name) -> None:
        super().__init__(LogType.CATALOG_MANAGER)
        schema = self.read_schema_file(file_name)
        self.schema = schema
        self.catalogs = {}
        self.log('Catalog Manager component initialized.')
            

