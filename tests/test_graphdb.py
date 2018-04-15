#!/usr/bin/env python
import unittest

from graphdb import GraphDB


# import pytest


class GraphDBTest(unittest.TestCase):
    def setUp(self):
        #print('setting up')
        self.db = GraphDB()

    def tearDown(self):
        #print('tearing down')
        self.db._destroy()

    def test_default_conn_count(self):
        self.assertEqual(len(self.db._connections), 1, 'incorrect default conn count')

    def test_store_item(self):
        for i in range(64):
            self.db.store_item(i)
            self.db.store_item(i)
        self.assertEqual(len(list(self.db.list_objects())), 64, 'wrong object count after inserts')

    def test_store_relation(self):
        for i in range(64):
            ii = i + 1
            self.db.store_relation(i, 'less_than', ii)

    def test_duplicate_store_relation(self):
        self.test_store_relation()
        self.test_store_relation()

    def test_relation_count_after_storing_relations(self):
        self.test_duplicate_store_relation()
        self.assertEqual(len(list(self.db.list_relations())), 64, 'wrong relation count after inserts')

    def test_object_count_after_storing_relations(self):
        self.test_duplicate_store_relation()
        self.assertEqual(len(list(self.db.list_objects())), 65, 'wrong object count after relation inserts')

    def test_single_traversal(self):
        self.test_store_relation()
        self.assertEqual(next(self.db(5).less_than()), 6, 'wrong value after single traversal')

    def test_multi_traversal(self):
        self.test_store_relation()
        self.assertEqual(
                next(self.db(10).less_than.less_than.less_than.less_than.less_than()),
                15,
                'wrong value after multi-traversal'
        )

    def test_circular_storage(self):
        circle = (
            ('joe', 'oliver'),
            ('oliver', 'billy'),
            ('billy', 'joe'),
            ('bob', 'joe'),
            ('bob', 'elliot'),
            ('elliot', 'joe'),
        )
        for _ in range(5):
            for a, b in circle:
                self.db(a).knows = b
            self.assertEqual(len(list(self.db.list_objects())), 5, 'wrong object count after circular insert')
            self.assertEqual(len(list(self.db.list_relations())), 6, 'wrong relation count after circular insert')
            self.assertEqual(next(self.db('billy').knows.knows()), 'oliver', 'wrong person found after stepping twice')
            self.assertEqual(next(self.db('billy').knows.knows.knows.knows.knows.knows()), 'billy',
                    'wrong person found after circling twice')

    def test_serialization(self):
        targets = [
            'hello',
            5,
            74.98462,
            True,
            (4, 5),
            [9, 10],
            { 1, 7 },
            None
        ]
        for i in targets:
            self.assertEqual(
                    self.db.deserialize(self.db.serialize(i)),
                    i,
                    'loss of data found in serialize/deserialize'
            )


# @pytest.mark.skip('dunno what to do with this')
# def test_graphdb():
#     ''' use this function to ensure everything is working correctly with graphdb '''
#     db = GraphDB()
#
#     for i in range(1, 10):
#         src, dst = (i - 1, i)
#         #print(db._id_of(i))
#         print('testing', (src, 'precedes', dst))
#         db.store_relation(src, 'precedes', dst)
#         db.store_relation(src, 'even', (not src % 2))
#         db(src).odd = bool(src % 2)
#
#     print(6 in db)  # search the db to see if youve already stored something
#
#     #db.show_objects()
#     #db.show_relations()
#
#     for i in range(5):
#         for ii in db.find(i, 'precedes'):
#             print(i, ii)
#
#     print(list(db.relations_of(7)))
#     print(list(db[6].precedes()))
#     print(db[6].precedes.even.to(list))
#     print(list(db[6].precedes.even()))
#     print(db[6].precedes.precedes.to(list))
#     print(db[6].precedes.precedes.even.to(list))
#
#     seven = db[6].precedes
#     print(seven)
#     print(seven.to(list))
#     print('setting an attribute')
#
#     db.show_objects()
#     db.show_relations()
#     seven.prime = True
#     print(db[5].precedes.precedes.prime.to(list))
#     print(db._id_of(99))
#
#     for i in range(1, 5):
#         print(i)
#         db[5].greater_than = i
#
#     print(db[5].greater_than.to(list))
#
#     #db.show_objects()
#     #db.show_relations()
#     print(list(db.relations_of(5)))
#
#     print()
#
#     print(list(gen.chain(((r, i) for i in db.find(5, r)) for r in db.relations_of(5))))
#
#     for r in db.relations_of(5):
#         print(r)
#         print(list(db.find(5, r)))
#
#     print(db(5).greater_than(list))
#     print(db(5).greater_than.where(lambda i: i % 2 == 0)(list))
#     print(db(5).greater_than.precedes(list))
#     print(db(5).greater_than.precedes.precedes.precedes.precedes.precedes.precedes(list))
#
#     print(db(5).greater_than.where('even', lambda i: i == True)(list))
#     print(db(5).greater_than.where('even', bool)(list))
#
#     db.delete_relation(5, 'greater_than', 2)
#     db.delete_relation(5, 'greater_than', 2, 3)
#     db.delete_relation(5, 'greater_than')
#
#     db.show_relations()
#     print('-')
#     print(list(db.relations_of(5)))
#     print('-')
#     print(list(db.relations_of(5, True)))
#     print('-')
#     print(list(db.relations_to(5)))
#     print('-')
#     print(list(db.relations_to(5, True)))
#
#     db.replace_item(5, 'waffles')
#     db.delete_item(6)
#     db.show_relations()
#
#     for i in db:
#         print(i)
#
#     for i in db.list_relations():
#         print(i)
#
#     db._destroy()


if __name__ == '__main__':
    unittest.main(verbosity=2)
