import sys
from TransactionManager import TransactionManager
from CatalogManager import CatalogManager
from Common import Component, Table, Page
from Logger import LogType

class DbmsSimulator(Component):
    def get_table(self, table_key):
        return self.tables.get(table_key)
    
    def set_table(self, table_key, table):
        self.table[table_key] = table

    def __init__(self, schema_file_name) -> None:
        super().__init__(LogType.DBMS_SIMULATOR)    
        self.tables = {}
        self.transaction_manager = TransactionManager()
        self.catalog_manager = CatalogManager(schema_file_name)
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
    page_1 = Page('page_1')
    page_1.content = [1,2,3,4,5,6,7,8,9]
    page_2 = Page('page_2')
    page_2.content = [10,20,30,40,50,60,70,80,90]
    page_3 = Page('page_3')
    page_3.content = [100,101,102,103,200,300,400,500,600,700,800,900]
    page_4 = Page('page_4')
    page_4.content = [1000,2000,3000,4000,5000,6000,7000,8000,9000]
    page_5 = Page('page_5')
    page_5.content = [10000,20000,30000,40000,50000,60000,70000,80000,90000]

    primary_index = dbms.catalog_manager.get_auxiliary('table_key_1', 'intensity')
    primary_index.recreate([page_1, page_2, page_3, page_4, page_5])
    print('pages', primary_index.pages)
    print('page', primary_index.get(1))
    primary_index.recreate([page_2, page_3, page_5])
    print('pages', primary_index.pages)
    print('page', primary_index.get(101))

    # txns = txn_mgr.read_files(file_names)
    # if txn_processing_type == 'rr':
    #     txn_mgr.round_robin(txns)
    # elif txn_processing_type == 'ran':
    #     txn_mgr.random_read(txns)


if __name__ == "__main__":
    main()
