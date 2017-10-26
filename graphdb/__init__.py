# -*- coding: utf-8 -*-
# @Author: Cody Kochmann
# @Date:   2017-10-25 20:10:58
# @Last Modified by:   Cody Kochmann
# @Last Modified time: 2017-10-25 20:22:30

import sqlite3
import pickle
import hashlib
from base64 import b64encode as b64e, b64decode as b64d

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
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.cursor = self.conn.cursor()
        self.execute = self.cursor.execute
        self.fetchall = self.cursor.fetchall
        self.fetchone = self.cursor.fetchone
        for i in startup_sql:
            self.execute(i)

    @staticmethod
    def hash(target):
        # tiny hash function for sha256 checksums
        return hashlib.sha1(target).hexdigest()
        h = hashlib.new('sha256')
        try:
            pass
        except:
            try:
                return hashlib.sha1(hash(target)).hexdigest()
            except:
                return hashlib.sha1(hash(str(target))).hexdigest()
        exit('something went wrong')

    @staticmethod
    def serialize(item):
        return b64e(pickle.dumps(
            item,
            protocol=pickle.HIGHEST_PROTOCOL
        ))

    @staticmethod
    def load(item):
        return pickle.loads(b64d(item))

    def store_item(self, item):
        if self.id_of(item) is None:
            #print('storing item', item)
            blob = self.serialize(item)
            self.execute(
                'insert into objects (code) values (?);',
                (blob,)
            )

    def id_of(self, target):
        try:
            self.execute(
                'select id from objects where code=? limit 1;',
                (self.serialize(target),)
            )
            return self.fetchone()[0]
        except:
            return None

    def store_relation(self, src, name, dst):
        #print('storing relation', src, name, dst)
        self.store_item(src)
        self.store_item(dst)
        self.execute('''
        insert into relations (name, src, dst) values (?,?,?);
        ''', (
            name, self.id_of(src), self.id_of(dst)
        ))

    def find(self, target, relation):
        ''' returns back all elements the target has a relation to '''
        _ = self.execute('''
        select code from objects where id in (
            select dst from relations where src=? and name=?
        )
        ''', (self.id_of(target), relation))
        for i in _:
            yield self.load(i[0])

    def relations_of(self, target):
        ''' return a list of all relations of an object '''
        _ = self.execute('''
            select distinct name from relations where src=?
        ''', (self.id_of(target),))
        for i in _.fetchall():
            yield i[0]

    def connections_of(self, target):
        return gen.chain( ((r,i) for i in self.find(target,r)) for r in self.relations_of(target) )
        # this also worked but seemed like it was gonna be more work to parse
        # {r:self.find(target,r) for r in self.relations_of(target)}

    def show_objects(self):
        for i in self.execute('select * from objects'):
            _id, code = i
            print(_id, code, self.load(code))

    def show_relations(self):
        self.execute('select * from relations')
        for i in self.fetchall():
            #print(i)
            src, name, dst = i
            src = self.load(next(self.execute('select code from objects where id=?',(src,)))[0])
            dst = self.load(next(self.execute('select code from objects where id=?',(dst,)))[0])
            print(src, name, dst)

    def __getitem__(self, key):
        _id = self.id_of(key)
        return VList([V(self, key)])

import generators as gen

class V(object):
    """docstring for V"""
    __slots__ = ('_graph_value','_graph_db')

    def __init__(self, db, value):
        self._graph_db = db
        self._graph_value = value

    def __getattribute__(self, key):
        ''' this runs a query on the next step of the query '''
        #print('get', key)
        if key in V.__slots__ or key == '__slots__':
            return object.__getattribute__(self, key)
        else:
            return VList(V(self._graph_db, _) for _ in self._graph_db.find(self._graph_value, key))
            #return V(self._graph_db, next(self._graph_db.find(self._graph_value, key), None))

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
    _slots = tuple(dir(list)) + ('_slots','to')

    def __init__(self, *args):
        list.__init__(self, *args)
        for i in self:
            #print(type(i))
            assert type(i) == V, 'needed a V and got a {}'.format(type(i))

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
            # below this seemed like a good idea but didnt work out too well :/
            # if g is only length of 1 just return that object
            #try:
            #    tmp_g = g()
            #    len_of_one=False
            #    next(tmp_g)
            #    len_of_one=True
            #    next(tmp_g)
            #    len_of_one=False
            #except:
            #    pass
            #finally:
            #    if len_of_one:
            #        return next(g())
            #    else:
            #        return VList(g())
            """
            for v in self:
                for fv in getattr(v, key):
                    yield fv()

            return VList(gen.chain((V(v._graph_db,fv) for fv in v._graph_db.find(v._graph_value, key)) for v in self))
            """
    def __call__(self):
        # load all values where this is called
        for i in self:
            yield i()


if __name__ == '__main__':

    db = GraphDB()

    for i in range(1,10):
        src,dst=(i-1,i)
        #print(db.id_of(i))
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
    print(db.id_of(99))



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
