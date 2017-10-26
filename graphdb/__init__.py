# -*- coding: utf-8 -*-
# @Author: Cody Kochmann
# @Date:   2017-10-25 20:10:58
# @Last Modified 2017-10-26
# @Last Modified time: 2017-10-26 15:06:31

from __future__ import print_function, unicode_literals
del print_function
from base64 import b64encode as b64e, b64decode as b64d
import generators as gen
import hashlib
import pickle
import sqlite3

''' sqlite based graph database for storing native python objects and their relationships to each other '''

__todo__ = '''
- have different relations stored in generated
  tables produced from the hash of the relation
  so lookup times arent as affected by scale
- set up a view between objects and relations
- add type enforcement to match relationships with attributes
'''

startup_sql='''
CREATE TABLE if not exists objects (
    id integer primary key autoincrement,
    code text not null,
    unique(code) on conflict ignore
);
''','''
CREATE TABLE if not exists relations (
    src int not null,
    name text not null,
    dst int not null,
    unique(src, name, dst) on conflict ignore,
    foreign key(src) references objects(id),
    foreign key(dst) references objects(id)
);
'''


class GraphDB(object):
    ''' sqlite based graph database for storing native python objects and their relationships to each other '''

    def __init__(self, path=':memory:'):
        self._path = path
        self._conn = sqlite3.connect(self._path)
        self._commit = self._conn.commit
        self._cursor = self._conn.cursor()
        self._execute = self._cursor.execute
        self._fetchall = self._cursor.fetchall
        self._fetchone = self._cursor.fetchone
        for i in startup_sql:
            self._execute(i)

    @staticmethod
    def _serialize(item):
        # b64e is used on top of dumps because python loses data when encoding
        # pickled objects for sqlite
        return b64e(pickle.dumps(
            item,
            protocol=pickle.HIGHEST_PROTOCOL
        ))

    @staticmethod
    def _load(item):
        return pickle.loads(b64d(item))

    def store_item(self, item):
        ''' use this function to store a python object in the database '''
        if self._id_of(item) is None:
            #print('storing item', item)
            blob = self._serialize(item)
            self._execute(
                'insert into objects (code) values (?);',
                (blob,)
            )
            self._commit()

    def _id_of(self, target):
        try:
            self._execute(
                'select id from objects where code=? limit 1;',
                (self._serialize(target),)
            )
            return self._fetchone()[0]
        except:
            return None

    def store_relation(self, src, name, dst):
        ''' use this to store a relation between two objects '''
        assert type(name).__name__ in {'str','unicode'}, 'name needs to be a string'
        #print('storing relation', src, name, dst)
        # make sure both items are stored
        self.store_item(src)
        self.store_item(dst)
        # run the insertion
        self._execute('''
        insert into relations (name, src, dst) values (?,?,?);
        ''', (
            name, self._id_of(src), self._id_of(dst)
        ))
        self._commit()

    def find(self, target, relation):
        ''' returns back all elements the target has a relation to '''
        _ = self._execute('''
        select code from objects where id in (
            select dst from relations where src=? and name=?
        )
        ''', (self._id_of(target), relation))
        for i in _:
            yield self._load(i[0])

    def relations_of(self, target):
        ''' list all relations for an object '''
        _ = self._execute('''
            select distinct name from relations where src=?
        ''', (self._id_of(target),))
        for i in _.fetchall():
            yield i[0]

    def connections_of(self, target):
        ''' generate tuples containing (relation, object_that_applies) '''
        return gen.chain( ((r,i) for i in self.find(target,r)) for r in self.relations_of(target) )
        # this also worked but seemed like it was gonna be more work to parse
        # {r:self.find(target,r) for r in self.relations_of(target)}

    def show_objects(self):
        ''' display the entire of objects with their (id, serialized_form, actual_value) '''
        for i in self._execute('select * from objects'):
            _id, code = i
            print(_id, code, self._load(code))

    def show_relations(self):
        ''' display every relation in the database as (src, relation, dst) '''
        self._execute('select * from relations')
        for i in self._fetchall():
            #print(i)
            src, name, dst = i
            src = self._load(next(self._execute('select code from objects where id=?',(src,)))[0])
            dst = self._load(next(self._execute('select code from objects where id=?',(dst,)))[0])
            print(src, name, dst)

    def __getitem__(self, key):
        return VList([V(self, key)])

    def __call__(self, key):
        return VList([V(self, key)])



class V(object):
    """docstring for V"""
    __slots__ = ('_graph_value','_graph_db','_relations')

    def __init__(self, db, value):
        self._graph_db = db
        self._graph_value = value
        self._relations = lambda s=self: list(s._graph_db.relations_of(s._graph_value))
        #self._connections = lambda db=db, s=self: db.connections_of(s)

    def __getattribute__(self, key):
        ''' this runs a query on the next step of the query '''
        #print('get', key)
        if key in V.__slots__ or key == '__slots__':
            return object.__getattribute__(self, key)
        else:
            return VList(V(self._graph_db, _) for _ in self._graph_db.find(self._graph_value, key))
            #return V(self._graph_db, next(self._graph_db.find(self._graph_value, key), None))

    __getitem__ = __getattribute__

    def to(self, output_type):
        assert type(output_type) == type, 'needed a type here not: {}'.format(output_type)
        return output_type([self()])

    def __setattr__(self, key, value):
        #print('set', key, value)
        if type(value) == VList:
            # create a relation to every object in the VList
            for i in value():
                setattr(self, key, i)
        if type(value) == V:
            value = value() # load the actual value if the target is this specific class
        if key in self.__slots__:
            object.__setattr__(self, key, value)
        else:
            self._graph_db.store_relation(self(), key, value)

    def __call__(self):
        return self._graph_value

class VList(list):
    _slots = tuple(dir(list)) + ('_slots','to','where')

    def __init__(self, *args):
        list.__init__(self, *args)
        for i in self:
            #print(type(i))
            assert type(i) == V, 'needed a V and got a {}'.format(type(i))

    def where(self, *args):
        ''' use this to filter VLists, simply provide a filter function and what relation to apply it to '''
        assert len(args) in {1,2}, 'invalid number of arguments for where'
        if len(args) == 1:
            relation, filter_fn = '', args[0]
        elif len(args) == 2:
            relation, filter_fn = args

        assert callable(filter_fn), 'VList.where needs filter_fn to be a callable function'
        assert type(relation).__name__ in {'str','unicode'}, 'where needs the first arg to be a string'

        def output(self=self, relation=relation, filter_fn=filter_fn):
            ''' this is an internal function because the output needs to be a VList but the problem is cleaner with a generator '''
            if len(relation): # if a relation is defined
                for i in self:
                    if relation in i._relations() and any(filter_fn(_()) for _ in i[relation]):
                        yield i
            else: # if no relation is defined, check against current values
                for i in self:
                    if filter_fn(i()):
                        yield i

        return VList(output())



    def to(self, output_type):
        assert type(output_type) == type, 'needed a type here not: {}'.format(output_type)
        return output_type(self())

    def __setattr__(self, key, value):
        #print('set', key, value)
        if type(value) == VList:
            # create a relation to every object in the VList
            for v in self():
                setattr(self, key, i)
        if type(value) == V:
            value = value() # load the actual value if the target is this specific class
        if key in self._slots:
            object.__setattr__(self, key, value)
        else:
            for v in self:
                setattr(v, key, value)
            #self._graph_db.store_relation(self(), key, value)

    def __getattribute__(self, key):
        if key in VList._slots:
            return object.__getattribute__(self, key)
        else:
            # run the attribute query on all elements in self
            g = lambda:gen.chain( (fv for fv in getattr(v,key)) for v in self )
            return VList(g())

    __getitem__ = __getattribute__

    def __call__(self, output_type=None):
        # load all values where this is called
        if output_type is None:
            return (i() for i in self)
        else:
            return output_type(i() for i in self)

def run_tests():
    ''' use this function to ensure everything is working correctly with graphdb '''
    db = GraphDB()

    for i in range(1,10):
        src,dst=(i-1,i)
        #print(db._id_of(i))
        db.store_relation(src, 'proceeds', dst)
        db.store_relation(src, 'even', (not src%2))
        db.store_relation(src, 'odd', bool(src%2))

    #db.show_objects()
    #db.show_relations()

    for i in range(5):
        for ii in db.find(i, 'proceeds'):
            print(i, ii)

    print(list(db.relations_of(7)))
    print(list(db[6].proceeds()))
    print(db[6].proceeds.even.to(list))
    print(list(db[6].proceeds.even()))
    print(db[6].proceeds.proceeds.to(list))
    print(db[6].proceeds.proceeds.even.to(list))

    seven = db[6].proceeds
    print(seven)
    print(seven.to(list))
    print('setting an attribute')


    db.show_objects()
    db.show_relations()
    seven.prime = True
    print(db[5].proceeds.proceeds.prime.to(list))
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
    print(db(5).greater_than.proceeds(list))
    print(db(5).greater_than.proceeds.proceeds.proceeds.proceeds.proceeds.proceeds(list))

    print(db(5).greater_than.where('even', lambda i:i==True)(list))
    print(db(5).greater_than.where('even', bool)(list))



if __name__ == '__main__':
    run_tests()
