class DTO:
    def __init__(self):
        pass

    def from_dict(self, data):
        for name, value in data.items():
            setattr(self, name, value)
        return self
