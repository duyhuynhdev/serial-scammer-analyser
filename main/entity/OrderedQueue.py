from queue import Queue

from marshmallow.orderedset import OrderedSet


class OrderedSetQueue(Queue):
    addresses = set()

    def _init(self, maxsize: int = 0):
        self.queue = OrderedSet()

    def _put(self, item):
        if item.address not in self.addresses:
            self.queue.add(item)
            self.addresses.add(item.address)

    def _get(self):
        item = self.queue.pop()
        self.addresses.remove(item.address)
        return item

    def __contains__(self, item):
        with self.mutex:
            return item in self.queue
