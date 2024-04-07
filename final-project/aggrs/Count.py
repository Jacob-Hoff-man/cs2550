from Common import Aggregate

class ClusteredIndex(Aggregate):
    def set(self, key, page_key):
        self.pages[key] = page_key

    def get(self, key):
        return self.pages.get(key)
    
    def __init__(self) -> None:
        self.pages = {}