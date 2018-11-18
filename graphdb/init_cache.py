from itertools import count

class Dog(object):
    counter = count()
    cache = {}

    def __init__(self, arg):
        print(id(arg))
        if arg in Dog.cache:
            print('grabbing cached')
            self.__dict__.update(Dog.cache[arg].__dict__)
        else:
            print('adding to cache')
            self.arg = arg
            self.number = next(Dog.counter)
            Dog.cache[arg] = self

for i in range(3):
    Dog(i)
for i in range(3):
    Dog(i)
for i in range(3):
    Dog(i)