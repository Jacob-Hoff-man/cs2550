import sys
from TransactionManager import TransactionManager
from CatalogManager import CatalogManager
from Common import Component
from Logger import LogType

class DbmsSimulator(Component):
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
    dbms.catalog_manager.insert_catalog('table_key')
    print(dbms.catalog_manager.schema)


    # txns = txn_mgr.read_files(file_names)

    # if txn_processing_type == 'rr':
    #     txn_mgr.round_robin(txns)
    # elif txn_processing_type == 'ran':
    #     txn_mgr.random_read(txns)


if __name__ == "__main__":
    main()
