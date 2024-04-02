def get_parts(v_addr):
    offset_bits = 13
    virtual_page_number_bits = 19
    root_number_bits = 9
    leaf_number_bits = 10
    virtual_page_number = (v_addr >> offset_bits) & (
        (1 << virtual_page_number_bits) - 1)
    root_number = (virtual_page_number >> leaf_number_bits) & (
        (1 << root_number_bits) - 1)
    leaf_number = virtual_page_number & ((1 << leaf_number_bits) - 1)
    return virtual_page_number, root_number, leaf_number

class ArrayEntry:
    def __init__(self, page_num, bit):
        self.page_num = page_num
        self.bit = bit 

class PageTableEntry:
    def __init__(self, frame_num, next_use):
        self.dirty = False
        self.refer = False
        self.valid = False
        self.frame_num = frame_num
        self.next_use = next_use

class PageTable:
    def __init__(self):
        self.root = [None] * 512
        self.leaves = 0

    def get_entry(self, v_addr):
        if isinstance(v_addr, str):
            _, root_idx, leaf_idx = get_parts(int(v_addr, 16))
        else:
            _, root_idx, leaf_idx = get_parts(v_addr)
        # print("root, leaf_idx: ", root_idx, leaf_idx)

        if self.root[root_idx] is None:
            self.root[root_idx] = [None] * 1024
            self.leaves += 1
        entr = self.root[root_idx][leaf_idx]

        return entr

    def set_entry(self, v_addr, page_table_entry):
        page_table_entry.valid = True
        _, root_idx, leaf_idx = GetParts(v_addr)

        if self.root[root_idx] is None:
            self.root[root_idx] = [None] * 1024
            self.leaves += 1
        self.root[root_idx][leaf_idx] = page_table_entry

class ColBuffer():
    def __init__(self, size) -> None:
        self.buffer = [None] * size
        self.max = size - 1 # max index in buffer (used by page table)

    def do_op(self, op):
        # data manager will send ops to buffer such as flushing 
        # or sending to row buffer
        return
    def flush(self, idx):
        # flushes this frame
        # if dirty need to write back/through to disc
        return
    def set(self, idx, page):
        # sets idx to page
        return
    def to_row(self, idx):
        # create the record
        # return it to data manager or can send to directly to row buffer
        return

class RowBuffer():
    def __init__(self, size) -> None:
        self.buffer = [None]*size
        self.max = size
    def do_op(self, op):
        # data manager will send ops to buffer such as flushing 
        # or sending to row buffer
        return
    def flush(self, idx):
        # flushes this frame
        # if dirty need to write back/through to disc
        return
    def set(self, idx, page):
        # sets idx to page
        return
    def to_col(self, idx):
        # create the columns
        # return it to data manager or can send to directly to col buffer
        return

class DataManager():
    def __init__(self, tb) -> None:
        self.tb = None
        self.attributes = None
        
    def do_op(self, op):
        # does an operation
        # switch case
        # helper for each op
        return

