import sys

from CatalogManager import CatalogManager
from Common import Component, Page, Table
from DataManager import DataManager
from Logger import LogType
from TransactionManager import TransactionManager


class DbmsSimulator(Component):
    def get_table(self, table_key):
        return self.tables.get(table_key)
    
    def set_table(self, table_key, table):
        self.tables[table_key] = table

    def __init__(self, schema_file_name) -> None:
        super().__init__(LogType.DBMS_SIMULATOR)    
        self.tables = {}
        self.transaction_manager = TransactionManager()
        self.catalog_manager = CatalogManager(schema_file_name)
        self.data_manager = DataManager(self.catalog_manager, self.tables, 4)
        self.log('DBMS Simulator component initialized.')

def main():
    schema_file_name = sys.argv[1]
    transaction_processing_type = sys.argv[2]
    file_names = sys.argv[3:]
    dbms = DbmsSimulator(schema_file_name)
    dbms.transaction_manager.read_files(file_names)
    dbms.catalog_manager.insert_catalog('table_key_1')
    print(dbms.catalog_manager.schema)

    # Count example
    aggregate = dbms.catalog_manager.get_aggregate('table_key_1', 'intensity')
    aggregate.increment(5)
    aggregate.decrement(5)
    print('aggr result', aggregate.get(5))
    aggregate.decrement(5)
    print('aggr result', aggregate.get(5))


    # Primary Index example
    # only need to use get and set when utilizing, it will recreate automatically
    # if you want to delete, use set(anchor, page_number=None)
    page_1 = (1, 20)
    page_2 = (10, 23)
    page_3 = (100, 41)
    page_4 = (1000, 620)
    page_5 = (10000, 21)
    page_6 = (100000, 57)
    primary_index = dbms.catalog_manager.get_auxiliary('table_key_1', 'intensity')
    primary_index.recreate([page_1, page_2, page_3, page_4, page_5])
    primary_index.set(101, 50)
    primary_index.set(102, 50)
    primary_index.set(103, 50)
    print('pages', primary_index.page_numbers, primary_index.overflows)
    primary_index.set(105, None)
    primary_index.set(103, None)
    primary_index.set(1000, None)
    print('pages', primary_index.page_numbers, primary_index.overflows)
    print('page', primary_index.get(3))
    primary_index.recreate([page_2, page_3, page_5])
    print('pages!', primary_index.page_numbers)
    print('page', primary_index.get(101))

    # txns = txn_mgr.read_files(file_names)
    # if txn_processing_type == 'rr':
    #     txn_mgr.round_robin(txns)
    # elif txn_processing_type == 'ran':
    #     txn_mgr.random_read(txns)


if __name__ == "__main__":
    main()
