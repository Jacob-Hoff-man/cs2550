from Common import Auxiliary, Page

class PrimaryIndex(Auxiliary):
    def recreate(self, pages):
        new_pages = {}
        for page in pages:
            new_pages[page.content[0]] = page
        self.pages = new_pages
        self.overflows = {}

    def is_overflow_limit_reached(self):
        return len(self.overflows.items()) == self.overflows
    
    def set(self, key, page_key):
        self.pages[key] = page_key

    def get(self, value):
        page = self.pages.get(value)
        if page == None:
            print('page not found yet')
            last_index_key = None
            for index_key in self.pages.keys():
                if index_key > value:
                    if last_index_key == None:
                        return None
                    value_page = self.pages.get(last_index_key)
                    if value in value_page.content:
                        return value_page
                last_index_key = index_key
            # find key that is 
    
    def __init__(self) -> None:
        self.pages= {}
        self.overflows = {}
        self.overflow_limit = 3