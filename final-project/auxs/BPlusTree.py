from bplustree import BPlusTree as Bpt
from Common import Auxiliary

class BPlusTree(AuxiliaryStructure):
    def set(self, key, page_key):
        self.pages[key] = page_key

    def get(self, key):
        return self.pages.get(key)
    
    def __init__(self) -> None:
        self.pages = Bpt('/tmp/bplustree.db', order=50)
