class OrderedQueue():
    def __init__(self):
        self.queue = list()
        self.addresses = set()

    def put(self, item):
        if item.address not in self.addresses:
            self.queue.append(item)
            self.addresses.add(item.address)

    def get(self):
        if len(self.queue) == 0:
            return None
        item = self.queue.pop(0)
        self.addresses.remove(item.address)
        return item

    def empty(self):
        return len(self.addresses) == 0

    def qsize(self):
        return len(self.addresses)