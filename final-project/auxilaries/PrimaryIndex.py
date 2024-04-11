from Common import Auxiliary, Page

class PrimaryIndex(Auxiliary):
    def recreate(self, page_numbers):
        new_page_numbers = {}
        for page_number in page_numbers:
            new_page_numbers[page_numbers] = page_number
        for overflow in self.overflows:
            overflow_anchor = overflow[0]
            overflow_page_number = overflow[1]
            new_page_numbers[overflow_anchor] = overflow_page_number
        self.page_numbers = new_page_numbers
        self.overflows = {}

    def is_overflow_limit_reached(self):
        return len(self.overflows.items()) == self.overflows

    def get_closest_page(self, value):
        page_number = self.page_numbers.get(value)
        if page_number == None:
            last_anchor = None
            for anchor in self.page_numbers.keys().sort():
                if anchor > value:
                    if last_anchor == None:
                        return None
                    closest_page = self.page_numbers.get(last_anchor)
                    overflows = self.overflows.get(last_anchor)
                    if overflows == None:
                        return closest_page
                    else:
                        last_overflow_page_number = None
                        for page_numbers in overflows.keys().sort():
                            overflow_anchor = page_numbers[0]
                            overflow_page_number = page_numbers[1]
                            if overflow_anchor > value:
                                if last_overflow_page_number == None:
                                    return closest_page
                                return last_overflow_page_number
                            elif overflow_anchor == value:
                                return overflow_page_number
                            last_overflow_page_number = overflow_page_number

                last_anchor = anchor
        else:
            return page_number
    
    def get_index(self):
        return self.page_numbers.keys().sort()
    
    # TODO: WIP
    def set(self, value):
        value_page = self.get_closest_page(value)
        if value_page.is_page_limit_reached():
            if self.is_overflow_limit_reached():
                return False
                # recreate  
            else:
            # new overflow bucket for (anchor_val, page_num)
        else:
        
    def get(self, value):
        value_page = self.get_closest_page(value)
        if value in value_page.content:
            return value_page
        return None
    
    def __init__(self) -> None:
        self.page_numbers = {}
        self.overflows = {}
        self.overflow_limit = 3