import json
from Common import FilterType, AuxType, ViewType

class Catalog():
    def __init__(self) -> None:
        self.auxs = {}
        self.filters = {}
        self.views = {}

class CatalogManager():
    def get_aux(self, table_key, column_key):
        return self.catalogs.get(table_key).auxs.get(column_key)

    def get_filter(self, table_key, column_key):
        return self.catalogs.get(table_key).filters.get(column_key)

    def get_view(self, table_key, column_key):
        return self.catalogs.get(table_key).views.get(column_key)
    
    def set_aux(self, aux: AuxType):
        match aux.value:
            case AuxType.CLUSTERED.value:
                # TODO
                return 'clustered_aux'
            case AuxType.ISAM.value:
                # TODO
                return 'isam_aux'
            case AuxType.R_TREE.value:
                # TODO
                return 'r_tree_aux'
            case AuxType.B_PLUS_TREE.value:
                # TODO
                return 'b_plus_tree_aux'
            case _:
                return 'none'

    def set_filter(self, filter: FilterType):
        match filter.value:
            case FilterType.BLOOM.value:
                # TODO
                return 'bloom_filter'
            case _:
                return 'none'

    def set_view(self, view: ViewType):
        match view.value:
            case ViewType.COUNT.value:
                # TODO
                return 'count_view'
            case _:
                return 'none'

    def insert_catalog(self, table_key):
        catalog = Catalog()
        for column_name, column_definition in self.schema.items():
            access_methods = column_definition.get('access_methods')
            if access_methods.get('filter') != None:
                column_filter = self.set_filter(FilterType(access_methods.get('filter')))
                catalog.filters[column_name] = column_filter
            if access_methods.get('auxiliary') != None:
                column_aux = self.set_aux(AuxType(access_methods.get('auxiliary')))
                catalog.auxs[column_name] = column_aux
            if access_methods.get('view') != None:
                column_view = self.set_view(ViewType(access_methods.get('view')))
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

            

