from Common import Aggregate

class Count(Aggregate):
    def increment(self, key):
        count = self.counts.get(key)
        if count == None:
            self.counts[key] = 1
        else:
            self.counts[key] = count + 1

    def decrement(self, key):
        count = self.counts.get(key)
        if count == None:
            return
        elif count == 1:
            del self.counts[key]
        else:
            self.counts[key] = count - 1

    def get(self, key):
        count = self.counts.get(key)
        if count == None:
            return 0
        else:
            return count
    
    def __init__(self) -> None:
        self.counts = {}