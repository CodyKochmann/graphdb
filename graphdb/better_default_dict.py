class better_default_dict(dict):
    def __init__(self, constructor):
        if not callable(constructor):
            constructor = lambda: constructor
        self._constructor = constructor

    def __getitem__(self, target):
        if target in self:
            retval = dict.__getitem__(self, target)
        else:
            dict.__setitem__(self, target, self._constructor())
            retval = dict.__getitem__(self, target)
        return retval
