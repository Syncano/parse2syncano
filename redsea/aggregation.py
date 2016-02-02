from collections import namedtuple

ClassAttach = namedtuple('ClassAttach', ('syncano_class', 'parse_class'))

class DataAggregated(object):

    def __init__(self):
        self.classes = []

    def add_class(self, syncano_class, parse_class):
        self.classes.append(ClassAttach(syncano_class, parse_class))
