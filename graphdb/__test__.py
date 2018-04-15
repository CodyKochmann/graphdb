import unittest, sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from __init__ import GraphDB

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
            ii=i+1
            self.db.store_relation(i,'less_than',ii)

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
            ('billy', 'joe')
        )
        for _ in range(5):
            for a,b in circle:
                self.db(a).knows = b
            self.assertEqual(len(list(self.db.list_objects())), 3, 'wrong object count after circular insert')
            self.assertEqual(len(list(self.db.list_relations())), 3, 'wrong relation count after circular insert')
            self.assertEqual(next(self.db('billy').knows.knows()), 'oliver', 'wrong person found after stepping twice')
            self.assertEqual(next(self.db('billy').knows.knows.knows.knows.knows.knows()), 'billy', 'wrong person found after circling twice')

    def test_serialization(self):
        targets=[
            'hello',
            5,
            74.98462,
            True,
            (4,5),
            [9,10],
            {1,7},
            None
        ]
        for i in targets:
            self.assertEqual(
                self.db.deserialize(self.db.serialize(i)),
                i,
                'loss of data found in serialize/deserialize'
            )

    def test_loadbalancer_example(self):
        ''' this builds an example mapping of a loadbalancer setup '''
        self.db('cluster').entry_point = 'loadbalancer-1'
        self.db('cluster').entry_point = 'loadbalancer-2'

        self.db('loadbalancer-1').connected_to = 'server-1'
        self.db('loadbalancer-1').connected_to = 'server-2'
        self.db('loadbalancer-1').connected_to = 'server-3'
        self.db('loadbalancer-1').connected_to = 'server-4'

        self.db('loadbalancer-2').connected_to = 'server-3'
        self.db('loadbalancer-2').connected_to = 'server-4'
        self.db('loadbalancer-2').connected_to = 'server-5'
        self.db('loadbalancer-2').connected_to = 'server-6'

    def test_where_filter(self):
        self.test_loadbalancer_example()

        self.assertEqual(
            self.db('cluster').entry_point.where('connected_to', lambda i:i=='server-2')(list),
            ['loadbalancer-1'],
            'wrong set of loadbalancers found'
        )
        self.assertEqual(
            self.db('cluster').entry_point.where('connected_to', lambda i:i=='server-3')(list),
            ['loadbalancer-1', 'loadbalancer-2'],
            'wrong set of loadbalancers found'
        )
        self.assertEqual(
            self.db('cluster').entry_point.where('connected_to', lambda i:i=='server-4')(list),
            ['loadbalancer-1', 'loadbalancer-2'],
            'wrong set of loadbalancers found'
        )
        self.assertEqual(
            self.db('cluster').entry_point.where('connected_to', lambda i:i=='server-5')(list),
            ['loadbalancer-2'],
            'wrong set of loadbalancers found'
        )

    def test_relations_to(self):
        self.test_loadbalancer_example()

        self.assertEqual(
            list(self.db.relations_to('server-3')),
            ['connected_to'],
            'wrong relations were found for "server-3"'
        )
        self.assertEqual(
            list(self.db.relations_to('server-3', include_object=True)),
            [('loadbalancer-1', 'connected_to'), ('loadbalancer-2', 'connected_to')],
            'wrong relations and objects were found for "server-3"'
        )

'''
# this code is for later to test if lambdas/functions/classes
# maintain functionality after serialization
{
    'complex':(lambda i=5:i*2),
    'dict':['hi',6,None,99.0]
}
lambda i:'hello {}'.format(i),
'''


if __name__ == '__main__':
    unittest.main()
