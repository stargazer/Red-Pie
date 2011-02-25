import time, types

class timing:
    """ Decorator:
        Will be used to log the total time that a function
        has spent, for all its calls.
    """
    def __init__(self, f):
        self.total = 0
        self.f = f
    
    def __call__(self, *args, **kwargs):
        start = time.clock()
        ret = self.f(*args, **kwargs)
        end = time.clock() - start
        self.total += end
        print(self.f.__name__,self.total)
        return ret
    
    def __get__(self, instance, owner = None):
        return types.MethodType(self, instance)
