import pprint

class A:
    def __init__(self):
        self.a = "A"

class B:
    def __init__(self):
        self.b = A()
    
    def __repr__(self):
        return "{\t}" + self.b.a

print(repr(B()))
print(str([1,2,3,4,5]))