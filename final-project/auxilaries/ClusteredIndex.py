from Common import Auxiliary

class ClusteredIndex(Auxiliary):
    def set(self, anchor, page_number):
        if page_number == None:
            del self.page_numbers[anchor]
        else:
            self.page_numbers[anchor] = page_number

    def get(self, anchor):
        return self.pages.get(anchor)
    
    def __init__(self) -> None:
        self.page_numbers = {}