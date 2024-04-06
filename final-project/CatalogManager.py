import json
from Common import FilterType, AuxType, ViewType, Auxiliary
from auxs.ClusteredIndex import ClusteredIndex
from auxs.BPlusTree import BPlusTree

class Catalog():
    def __init__(self) -> None:
        self.auxs = {}
        self.filters = {}
        self.views = {}

class CatalogManager():
    def get_aux(self, table_key, column_key) -> Auxiliary:
        return self.catalogs.get(table_key).auxs.get(column_key)

    def get_filter(self, table_key, column_key):
        return self.catalogs.get(table_key).filters.get(column_key)

    def get_view(self, table_key, column_key):
        return self.catalogs.get(table_key).views.get(column_key)

    def set_aux(self, table_key, column_key, aux):
        self.catalogs[table_key].auxs[column_key] = aux

    def set_filter(self, table_key, column_key, filter):
        self.catalogs[table_key].filters[column_key] = filter

    def set_view(self, table_key, column_key, view):
        self.catalogs[table_key].views[column_key] = view

    def insert_catalog(self, table_key):
        def get_catalog_aux(aux: AuxType):
            match aux.value:
                case AuxType.CLUSTERED.value:
                    return ClusteredIndex()
                case AuxType.ISAM.value:
                    # TODO
                    return None
                case AuxType.R_TREE.value:
                    # TODO
                    return None
                case AuxType.B_PLUS_TREE.value:
                    # TODO: fix issue with BPlusTree().set throwing error
                    # return BPlusTree()
                    return None
                case _:
                    return None

        def get_catalog_filter(filter: FilterType):
            match filter.value:
                case FilterType.BLOOM.value:
                    # TODO
                    return None
                case _:
                   return None

        def get_catalog_view(view: ViewType):
            match view.value:
                case ViewType.COUNT.value:
                    # TODO
                    return None
                case _:
                    return None

        catalog = Catalog()
        for column_name, column_definition in self.schema.items():
            access_methods = column_definition.get('access_methods')
            if access_methods.get('filter') != None:
                column_filter = get_catalog_filter(FilterType(access_methods.get('filter')))
                catalog.filters[column_name] = column_filter
            if access_methods.get('auxiliary') != None:
                column_aux = get_catalog_aux(AuxType(access_methods.get('auxiliary')))
                catalog.auxs[column_name] = column_aux
            if access_methods.get('view') != None:
                column_view = get_catalog_view(ViewType(access_methods.get('view')))
                catalog.views[column_name] = column_view
        self.catalogs[table_key] = catalog

    def delete_catalog(self, table_key):
        del self.catalogs[table_key]

    def read_schema_file(self, file_name):
        f = open(file_name)
        data = json.load(f)
        f.close()
        return data

    def __init__(self, file_name) -> None:
        schema = self.read_schema_file(file_name)
        self.schema = schema
        self.catalogs = {}

            

