from Common import Auxiliary, Page


class PrimaryIndex(Auxiliary):
    def recreate(self, page_numbers):
        new_page_numbers = {}
        for (anchor, page_number) in page_numbers:
            if page_number != None:
                new_page_numbers[anchor] = page_number
        for (anchor, overflows) in self.overflows.items():
            for (overflow_anchor, overflow_page_number) in overflows:
                new_page_numbers[overflow_anchor] = overflow_page_number
        self.page_numbers = new_page_numbers
        self.overflows = {}

    def is_overflow_limit_reached(self):
        overflow_count = 0
        for (anchor, overflows) in self.overflows.items():
            overflow_count += len(overflows)
        return overflow_count >= self.overflow_limit

    def get_index(self):
        index = list(self.page_numbers.keys())
        index.sort()
        return index

    def get_closest_page_number(self, value, check_overflow=True):
        page_number = self.page_numbers.get(value)
        if page_number == None:
            last_anchor = None
            index_structure = self.get_index()
            for anchor in index_structure:
                if anchor > value:
                    if last_anchor == None:
                        return None
                    closest_page = self.page_numbers.get(last_anchor)
                    overflows = self.overflows.get(last_anchor)
                    if overflows == None or not check_overflow:
                        return (last_anchor, closest_page)
                    else:
                        last_overflow_anchor = None
                        overflows.sort()
                        for (overflow_anchor, overflow_page_number) in overflows:
                            if overflow_anchor > value:
                                if last_overflow_anchor == None:
                                    return (last_anchor, closest_page)
                                return (last_overflow_anchor, self.overflows.get(last_overflow_anchor))
                            elif overflow_anchor == value:
                                return (overflow_anchor, overflow_page_number)
                            last_overflow_anchor = overflow_anchor

                last_anchor = anchor
        else:
            return (value, page_number)

    def get(self, value):
        return self.get_closest_page_number(value)

    def set(self, anchor, page_number):
        if self.page_numbers.get(anchor) != None:
            if page_number == None:
                del self.page_numbers[anchor]
            else:
                self.page_numbers[anchor] = page_number
        else:
            (closest_anchor, closest_page_number) = self.get_closest_page_number(
                anchor, False)
            overflow = self.overflows.get(closest_anchor)
            if overflow == None:
                if page_number != None:
                    self.overflows[closest_anchor] = [(anchor, page_number)]
            else:
                if self.is_overflow_limit_reached():
                    if page_number == None:
                        recreate_page_numbers = []
                    else:
                        recreate_page_numbers = [(anchor, page_number)]
                    index = self.get_index()
                    for index_anchor in index:
                        index_page_number = self.page_numbers.get(index_anchor)
                        recreate_page_numbers.append(
                            (index_anchor, index_page_number))
                    self.recreate(recreate_page_numbers)
                else:
                    new_overflow = [x for x in overflow if x[0] != anchor]
                    if page_number != None:
                        new_overflow.append((anchor, page_number))
                    self.overflows[closest_anchor] = new_overflow

    def __init__(self) -> None:
        self.page_numbers = {}
        self.overflows = {}
        self.overflow_limit = 3
