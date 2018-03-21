''' this script is used to run benchmarks on GraphDB '''
from __future__ import print_function
from itertools import count
from functools import partial
import unittest, sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from __init__ import GraphDB
from generators import rps, G

def report(name, speed):
    print('{:7}/sec - {}'.format(speed, name))
    
class GraphDBTest(unittest.TestCase):
    def setUp(self):
        #print('setting up')
        self.db = GraphDB()
        
    def tearDown(self):
        #print('tearing down')
        self.db._destroy()

    def test_insert_item(self):
        db=self.db
        report('object insertion', rps(
            G(count()).map(lambda i:db.store_item(i))
        ))
    
    def test_insert_relation(self):
        db=self.db
        #for i in range(20000):
        #    db.store_item(i)
        report('relation insertion', rps(
            G(count()).map(lambda i:db.store_relation(i,'less_than',i+1))
        ))
        
    def test_serialization(self):
        db=self.db
        report('serialization', rps(
            G(count()).map(lambda i:db.serialize(i))
        ))
        
    def test_serialization_deserialization(self):
        db=self.db
        report('serialization/deserialization', rps(
            G(count()).map(lambda i:db.deserialize(db.serialize(i)))
        ))
        
    def test_1_traversal(self):
        db=self.db
        db(5).under = 6
        report('1 step traversal', rps(
            iter((lambda:next(db(5).under())), 2)
        ))
    
    def test_2_traversal(self):
        db=self.db
        db(5).under = 6
        db(6).under = 7
        report('2 step traversal', rps(
            iter((lambda:next(db(5).under.under())), 2)
        ))
        
    def test_3_traversal(self):
        db=self.db
        db(5).under = 6
        db(6).under = 7
        db(7).under = 8
        report('3 step traversal', rps(
            iter((lambda:next(db(5).under.under.under())), 2)
        ))
    
    def test_4_traversal(self):
        db=self.db
        db(5).under = 6
        db(6).under = 7
        db(7).under = 8
        db(8).under = 9
        report('4 step traversal', rps(
            iter((lambda:next(db(5).under.under.under.under())), 2)
        ))
    
    def test_5_traversal(self):
        db=self.db
        db(5).under = 6
        db(6).under = 7
        db(7).under = 8
        db(8).under = 9
        db(9).under = 10
        report('5 step traversal', rps(
            iter((lambda:next(db(5).under.under.under.under.under())), 2)
        ))
        
    def test_6_traversal(self):
        db=self.db
        db(5).under = 6
        db(6).under = 7
        db(7).under = 8
        db(8).under = 9
        db(9).under = 10
        db(10).under = 11
        report('6 step traversal', rps(
            iter((lambda:next(db(5).under.under.under.under.under.under())), 2)
        ))
    
    def test_7_traversal(self):
        db=self.db
        db(5).under = 6
        db(6).under = 7
        db(7).under = 8
        db(8).under = 9
        db(9).under = 10
        db(10).under = 11
        db(11).under = 12
        report('7 step traversal', rps(
            iter((lambda:next(db(5).under.under.under.under.under.under.under())), 2)
        ))
    
    def test_7_traversal_circle(self):
        db=self.db
        db(5).under = 6
        db(6).under = 7
        db(7).under = 5
        report('7 step traversal (circular)', rps(
            iter((lambda:next(db(5).under.under.under.under.under.under.under())), 2)
        ))
        
if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=0).run(
        unittest.findTestCases(sys.modules[__name__])
    )

