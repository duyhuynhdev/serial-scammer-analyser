class Address:
    def __init__(self, address):
        self.address = address


class Account(Address):
    def __init__(self, address):
        super().__init__(address)


class Contract(Address):
    def __init__(self, address):
        super().__init__(address)


class ERC20(Contract):
    def __init__(self, address):
        super().__init__(address)


class Pool(ERC20):
    def __init__(self, address):
        super().__init__(address)


class Token(ERC20):

    def __init__(self, address):
        super().__init__(address)
