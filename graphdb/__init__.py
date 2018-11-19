# -*- coding: utf-8 -*-
# @Author: Cody Kochmann
# @Date:   2017-10-25 20:10:58
# @Last Modified 2018-03-19
# @Last Modified time: 2018-04-15 17:52:34

import sys, os
from functools import partial

import generators as gen

__all__ = ['GraphDB']

__module_path__ = os.path.dirname(os.path.abspath(__file__))
sys.path.append(__module_path__)

if sys.version_info > (3,0):
    from RamGraphDB import RamGraphDB
from SQLiteGraphDB import SQLiteGraphDB

def GraphDB(path=':memory:', autostore=True, autocommit=True):
    if path == ':memory:' and  sys.version_info > (3,0):
        return RamGraphDB(autostore=autostore)
    else:
        return SQLiteGraphDB(path=path, autostore=autostore, autocommit=autocommit)


def run_tests():
    ''' use this function to ensure everything is working correctly with graphdb '''
    db = GraphDB()

    for i in range(1,10):
        src,dst=(i-1,i)
        #print(db._id_of(i))
        print('testing',(src, 'precedes', dst))
        db.store_relation(src, 'precedes', dst)
        db.store_relation(src, 'even', (not src%2))
        db(src).odd = bool(src%2)

    print(6 in db) # search the db to see if youve already stored something

    #db.show_objects()
    #db.show_relations()

    for i in range(5):
        for ii in db.find(i, 'precedes'):
            print(i, ii)

    print(list(db.relations_of(7)))
    print(list(db[6].precedes()))
    print(db[6].precedes.even.to(list))
    print(list(db[6].precedes.even()))
    print(db[6].precedes.precedes.to(list))
    print(db[6].precedes.precedes.even.to(list))

    seven = db[6].precedes
    print(seven)
    print(seven.to(list))
    print('setting an attribute')


    db.show_objects()
    db.show_relations()
    seven.prime = True
    print(db[5].precedes.precedes.prime.to(list))
    print(db._id_of(99))

    for i in range(1,5):
        print(i)
        db[5].greater_than = i

    print(db[5].greater_than.to(list))

    #db.show_objects()
    #db.show_relations()
    print(list(db.relations_of(5)))

    print()

    print(list(gen.chain( ((r,i) for i in db.find(5,r)) for r in db.relations_of(5) )))

    for r in db.relations_of(5):
        print(r)
        print(list(db.find(5,r)))

    print(db(5).greater_than(list))
    print(db(5).greater_than.where(lambda i:i%2==0)(list))
    print(db(5).greater_than.precedes(list))
    print(db(5).greater_than.precedes.precedes.precedes.precedes.precedes.precedes(list))

    print(db(5).greater_than.where('even', lambda i:i==True)(list))
    print(db(5).greater_than.where('even', bool)(list))

    db.delete_relation(5, 'greater_than', 2)
    db.delete_relation(5, 'greater_than', 2)
    db.delete_relation(5, 'greater_than', 3)

    db.show_relations()
    print('-')
    print(list(db.relations_of(5)))
    print('-')
    print(list(db.relations_of(5, True)))
    print('-')
    print(list(db.relations_to(5)))
    print('-')
    print(list(db.relations_to(5, True)))

    db.replace_item(5, 'waffles')
    db.delete_item(6)
    db.show_relations()

    for i in db:
        print(i)

    for i in db.list_relations():
        print(i)

    db._destroy()

def run_unittests():
    print('\nrunning unittests...\n')
    import unittest, sys, os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    import __test__
    sys.path.pop()

    unittest.TextTestRunner(verbosity=2).run(
        unittest.findTestCases(__test__)
    )

def run_benchmarks():
    print('\nrunning benchmarks...\n')
    import unittest, sys, os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    import __benchmark__
    sys.path.pop()

    unittest.TextTestRunner(verbosity=0).run(
        unittest.findTestCases(__benchmark__)
    )

if __name__ == '__main__':
    run_tests()
    run_unittests()
    run_benchmarks()

