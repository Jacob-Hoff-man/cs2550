from Common import Auxiliary


class ClusteredIndex(Auxiliary):
    def set(self, anchor, page_number):
        if page_number == None:
            del self.page_numbers[anchor]
        else:
            if anchor in self.page_numbers.keys():
                self.page_numbers[anchor].append(page_number)
            else:
                self.page_numbers[anchor] = [page_number]

    def get(self, anchor):
        return self.page_numbers.get(anchor)

    def __init__(self) -> None:
        self.page_numbers = {}
