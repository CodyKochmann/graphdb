from __future__ import print_function, unicode_literals
del print_function
from base64 import b64encode as b64e, b64decode as b64d
import generators as gen
from generators.inline_tools import attempt
from strict_functions import overload
import hashlib
import dill
import sqlite3
from os import remove
from os.path import isfile

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


from threading import Lock, Semaphore

class read_write_state_machine(object):
    READ=0
    WRITE=1

    def __init__(self, readers=4, writers=1):
        self._state = self.WRITE
        self.readers = readers
        self.writers = writers
        self.state_setter_lock = Lock()
        self.states = {
            self.READ: Semaphore(readers),
            self.WRITE: Semaphore(writers)
        }
        self.state = self.READ

    @property
    def state_lock(self):
        return self.states[self._state]

    def acquire_current_locks(self):
        needed = self.writers if self._state else self.readers
        collected = 0
        #print(self.state_lock.__dict__)
        while collected < needed:
            self.state_lock.acquire()
            collected+=1

    def release_current_state(self):
        needed = self.writers if self._state else self.readers
        for i in range(needed):
            self.state_lock.release()
    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state):
        if new_state != self._state:
            try: # check if state is being changed
                self.state_setter_lock.acquire(blocking=False)
            except:
                # just wait for the change
                with self.states[new_state]:
                    pass
            else:
                if new_state != self._state:
                    #print('switching to write' if new_state else 'switching to read')
                    self.acquire_current_locks()
                    self._state = new_state
                    self.release_current_state()
                    #print('switched to write' if self._state else 'switched to read')
    @property
    def read(self):
        self.state=self.READ
        return self.states[self.READ]

    @property
    def write(self):
        self.state=self.WRITE
        return self.states[self.WRITE]

class better_default_dict(dict):
    def __init__(self, constructor):
        if not callable(constructor):
            constructor = lambda:constructor
        self._constructor = constructor

    def __getitem__(self, target):
        if target in self:
            return dict.__getitem__(self, target)
        else:
            dict.__setitem__(self, target, self._constructor())
            return dict.__getitem__(self, target)

from threading import current_thread

class SQLiteGraphDB(object):
    ''' sqlite based graph database for storing native python objects and their relationships to each other '''

    def __init__(self, path=':memory:', autostore=True, autocommit=True):
        if path != ':memory:':
            self._create_file(path)
        self._state = read_write_state_machine()
        self._autostore = True
        self._autocommit = True
        self._path = path
        self._connections = better_default_dict(lambda s=self:sqlite3.connect(s._path))
        self._cursors = better_default_dict(lambda s=self:s.conn.cursor())
        self._write_lock = Lock()
        with self._write_lock:
            for i in startup_sql:
                self._execute(i)
            self.commit()

    def close(self):
        for con in self._connections.values():
            con.close()

    def _destroy(self):
        self.close()
        if self._path != ':memory:' and isfile(self._path):
            remove(self._path)
        self.__dict__.clear()
        del self

    def _fetchone(self):
        return self._cursor.fetchone()

    def _fetchall(self):
        return self._cursor.fetchall()

    def _execute(self, *args):
        return self._cursor.execute(*args)

    @property
    def _cursor(self):
        return self._cursors[current_thread()]

    def commit(self):
        self.conn.commit()

    @property
    def conn(self):
        ''' return the connection for this thread '''
        return self._connections[hash(current_thread())]

    def autocommit(self):
        if self._autocommit:
            self.commit()

    @staticmethod
    def _create_file(path=''):
        ''' creates a file at the given path and sets the permissions to user only read/write '''
        from os.path import isfile
        if not isfile(path): # only do the following if the file doesn't exist yet
            from os import chmod
            from stat import S_IRUSR, S_IWUSR

            open(path, "a").close()  # create the file
            attempt(lambda: chmod(path, (S_IRUSR | S_IWUSR)))  # set read and write permissions

    @staticmethod
    def serialize(item):
        # b64e is used on top of dumps because python loses data when encoding
        # dilled objects for sqlite
        return b64e(dill.dumps(
            item,
            protocol=dill.HIGHEST_PROTOCOL
        ))

    @staticmethod
    def deserialize(item):
        return dill.loads(b64d(item))

    def store_item(self, item):
        ''' use this function to store a python object in the database '''
        #print('storing item', item)
        item_id = self._id_of(item)
        #print('item_id', item_id)
        if item_id is None:
            #print('storing item', item)
            blob = self.serialize(item)
            with self._write_lock:
                self._execute(
                    'INSERT into objects (code) values (?);',
                    (blob,)
                )
                self.autocommit()

    def delete_item(self, item):
        ''' removes an item from the db '''
        for relation in self.relations_of(item):
            self.delete_relation(item, relation)
        for origin, relation in self.relations_to(item, True):
            self.delete_relation(origin, relation, item)
        with self._write_lock:
            self._execute('''
                DELETE from objects where code=?
            ''', (self.serialize(item),))
            self.autocommit()

    def replace_item(self, old_item, new_item):
        if self._id_of(old_item) is not None: # if there is something to replace
            if self._id_of(new_item) is None: # if the replacement does not already exist
                with self._write_lock:
                    self._execute('''
                        UPDATE objects set code=? where code=?
                    ''', (self.serialize(new_item), self.serialize(old_item)))
                    self.autocommit()
            else: # if the replacement does exist, just move the links from old to new
                for relation, target in self.relations_of(old_item, True):
                    self.store_relation(new_item, relation, target)
                for origin, relation in self.relations_to(old_item, True):
                    self.store_relation(origin, relation, new_item)
                self.delete_item(old_item)

    def _id_of(self, target):
        try:
            self._execute(
                'select id from objects where code=? limit 1;',
                (self.serialize(target),)
            )
            return self._fetchone()[0]
        except:
            return None

    def __contains__(self, target):
        return self._id_of(target) != None

    def __iadd__(self, target):
        ''' use this to combine databases '''
        assert type(target) is self.__class__, 'graph databases can only be added to other graph databases'
        for src, name, dst in target.list_relations():
            self.store_relation(src, name, dst)

    def __add__(self, target):
        ''' use this to create a joined database from two graph databases '''
        assert type(target) is self.__class__, 'graph databases can only be added to other graph databases'
        out = SQLiteGraphDB()
        out += self
        out += target
        return out

    @staticmethod
    def __require_string__(target):
        assert type(target).__name__ in {'str','unicode'}, 'string required'

    def store_relation(self, src, name, dst):
        ''' use this to store a relation between two objects '''
        self.__require_string__(name)
        #print('storing relation', src, name, dst)
        # make sure both items are stored
        self.store_item(src)
        self.store_item(dst)
        with self._write_lock:
            #print(locals())
            # run the insertion
            self._execute(
                'insert into relations select ob1.id, ?, ob2.id from objects as ob1, objects as ob2 where ob1.code=? and ob2.code=?;',
                (name, self.serialize(src), self.serialize(dst))
            )
            self.autocommit()

    def _delete_single_relation(self, src, relation, dst):
        ''' deletes a single relation between objects '''
        self.__require_string__(relation)
        src_id = self._id_of(src)
        dst_id = self._id_of(dst)
        with self._write_lock:
            self._execute('''
                DELETE from relations where src=? and name=? and dst=?
            ''', (src_id, relation, dst_id))
            self.autocommit()

    def delete_relation(self, src, relation, *targets):
        ''' can be both used as (src, relation, dest) for a single relation or
            (src, relation) to delete all relations of that type from the src '''
        self.__require_string__(relation)
        if len(targets):
            for i in targets:
                self._delete_single_relation(src, relation, i)
        else:
            # delete all connections of that relation from src
            for i in list(self.find(src, relation)):
                self._delete_single_relation(src, relation, i)

    def find(self, target, relation):
        ''' returns back all elements the target has a relation to '''
        query = 'select ob1.code from objects as ob1, objects as ob2, relations where relations.dst=ob1.id and relations.name=? and relations.src=ob2.id and ob2.code=?' # src is id not source :/
        for i in self._execute(query, (relation, self.serialize(target))):
            yield self.deserialize(i[0])

    def relations_of(self, target, include_object=False):
        ''' list all relations the originate from target '''
        if include_object:
            _ = self._execute('''
                select relations.name, ob2.code from relations, objects as ob1, objects as ob2 where relations.src=ob1.id and ob2.id=relations.dst and ob1.code=?
            ''', (self.serialize(target),))
            for i in _:
                yield i[0], self.deserialize(i[1])
        else:

            _ = self._execute('''
                select distinct relations.name from relations, objects where relations.src=objects.id and objects.code=?
            ''', (self.serialize(target),))
            for i in _:
                yield i[0]

    def relations_to(self, target, include_object=False):
        ''' list all relations pointing at an object '''
        if include_object:
            _ = self._execute('''
                select name, (select code from objects where id=src) from relations where dst=?
            ''', (self._id_of(target),))
            for i in _:
                yield self.deserialize(i[1]), i[0]
        else:
            _ = self._execute('''
                select distinct name from relations where dst=?
            ''', (self._id_of(target),))
            for i in _:
                yield i[0]

    def connections_of(self, target):
        ''' generate tuples containing (relation, object_that_applies) '''
        return gen.chain( ((r,i) for i in self.find(target,r)) for r in self.relations_of(target) )

    def list_objects(self):
        ''' list the entire of objects with their (id, serialized_form, actual_value) '''
        for i in self._execute('select * from objects'):
            _id, code = i
            yield _id, code, self.deserialize(code)

    def __iter__(self):
        ''' iterate over all stored objects in the database '''
        for i in self._execute('select code from objects'):
            yield self.deserialize(i[0])

    def show_objects(self):
        ''' display the entire of objects with their (id, serialized_form, actual_value) '''
        for i in self.list_objects():
            print(*i)

    def list_relations(self):
        ''' list every relation in the database as (src, relation, dst) '''
        _ = self._execute('select * from relations').fetchall()
        for i in _:
            #print(i)
            src, name, dst = i
            src = self.deserialize(
                next(self._execute('select code from objects where id=?',(src,)))[0]
            )
            dst = self.deserialize(
                next(self._execute('select code from objects where id=?',(dst,)))[0]
            )
            yield src, name, dst

    def show_relations(self):
        ''' display every relation in the database as (src, relation, dst) '''
        for i in self.list_relations():
            print(*i)

    def __getitem__(self, key):
        if self._autostore:
            self.store_item(key)
        return VList([V(self, key)])

    def __call__(self, key):
        return VList([V(self, key)])



class V(object):
    '''docstring for V'''
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

    def where(self, relation, filter_fn):
        ''' use this to filter VLists, simply provide a filter function and what relation to apply it to '''
        assert type(relation).__name__ in {'str','unicode'}, 'where needs the first arg to be a string'
        assert callable(filter_fn), 'filter_fn needs to be callable'
        return VList(i for i in self if relation in i._relations() and any(filter_fn(_()) for _ in i[relation]))

    def _where(self, filter_fn):
        ''' use this to filter VLists, simply provide a filter function to filter the current found objects '''
        assert callable(filter_fn), 'filter_fn needs to be callable'
        return VList(i for i in self if filter_fn(i()))

    where = overload(_where, where)

    def _where(self, **kwargs):
        '''use this to filter VLists with kv pairs'''
        out = self
        for k,v in kwargs.items():
            out = out.where(k, lambda i:i==v)
        return out
    
    where = overload(_where, where)

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
