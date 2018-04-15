#!/usr/bin/env python
""" sqlite based graph database for storing native python objects and their relationships to each other """

from __future__ import print_function, unicode_literals

import logging
import sqlite3
from base64 import b64decode as b64d, b64encode as b64e
from os import chmod, remove
from os.path import isfile
from stat import S_IRUSR, S_IWUSR
from threading import Lock, Semaphore, current_thread

import dill
import generators as gen
from generators.inline_tools import attempt

from .better_default_dict import better_default_dict

del print_function  #MP  WTF?

# -*- coding: utf-8 -*-
# @Author: Cody Kochmann
# @Date:   2017-10-25 20:10:58
# @Last Modified 2018-03-19
# @Last Modified time: 2018-03-21 15:54:40

logger = logging.getLogger('GraphDB')
logger.setLevel(logging.INFO)

__todo__ = '''
- have different relations stored in generated
  tables produced from the hash of the relation
  so lookup times arent as affected by scale
- set up a view between objects and relations
- add type enforcement to match relationships with attributes
'''

startup_sql = ('''
CREATE TABLE if not exists objects (
    id integer primary key autoincrement,
    code text not null,
    unique(code) on conflict ignore
);''',
               '''
               CREATE TABLE if not exists relations (
                   src int not null,
                   name text not null,
                   dst int not null,
                   unique(src, name, dst) on conflict ignore,
                   foreign key(src) references objects(id),
                   foreign key(dst) references objects(id)
               );''')


class read_write_state_machine(object):
    #MP there are entire project that create very nice FSMs. maybe they're better?
    READ = 0
    WRITE = 1

    def __init__(self, readers=4, writers=1):
        self._state = self.WRITE
        self.readers = readers
        self.writers = writers
        self.state_setter_lock = Lock()
        self.states = {
            self.READ: Semaphore(readers),
            self.WRITE: Semaphore(writers)
        }  #MP why is this not enums?  later you have code doing checks that enums would enforce for you
        self.state = self.READ

    @property
    def state_lock(self):
        return self.states[self._state]

    @property
    def state(self):
        return self._state

    def acquire_current_locks(self):
        needed = self.writers if self._state else self.readers
        collected = 0
        logger.debug(self.state_lock.__dict__)
        while collected < needed:
            self.state_lock.acquire()
            collected += 1

    def release_current_state(self):
        needed = self.writers if self._state else self.readers
        for _ in range(needed):
            self.state_lock.release()

    @state.setter
    def state(self, new_state):
        if new_state != self._state:
            try:  # check if state is being changed
                self.state_setter_lock.acquire(blocking=False)
            except Exception:  #MP which exception?
                # just wait for the change
                with self.states[new_state]:
                    pass
            else:
                if new_state != self._state:
                    #MP contant value here?  already checked for it on the path to get here
                    logger.debug('switching to write' if new_state else 'switching to read')
                    self.acquire_current_locks()
                    self._state = new_state
                    self.release_current_state()
                    logger.debug('switched to write' if self._state else 'switched to read')

    @property
    def read(self):
        self.state = self.READ
        return self.states[self.READ]

    @property
    def write(self):
        self.state = self.WRITE
        return self.states[self.WRITE]


class GraphDB(object):
    """ sqlite based graph database for storing native python objects and their relationships to each other """

    def __init__(self, path=':memory:', autostore=True, autocommit=True):
        if path != ':memory:':
            self._create_file(path)
        self._path = path
        self._state = read_write_state_machine()
        self._autostore = autostore
        self._autocommit = autocommit

        self._connections = better_default_dict(lambda s=self: sqlite3.connect(s._path))
        self._cursors = better_default_dict(lambda s=self: s.conn.cursor())

        self._write_lock = Lock()  #MP is this DB-wide lock?
        with self._write_lock:
            for i in startup_sql:
                logger.debug(i)
                self._execute(i)
            self.commit()

    def _destroy(self):
        for con in self._connections.values():
            con.close()

        if self._path != ':memory:' and isfile(self._path):
            remove(self._path)
        self.__dict__.clear()
        del self

    def _fetchone(self):
        return self._cursor.fetchone()

    def _fetchall(self):
        return self._cursor.fetchall()

    def _execute(self, *args):
        logger.debug(*args)
        return self._cursor.execute(*args)

    @property
    def _cursor(self):
        return self._cursors[current_thread()]

    def commit(self):
        self.conn.commit()

    @property
    def conn(self):
        """ return the connection for this thread """
        return self._connections[hash(current_thread())]

    def autocommit(self):
        if self._autocommit:
            #MP this is not necessary. if autocommit in SQLite works correctly
            #MP explicit commits are redundant, and possibly causing extra locking/syncing
            #MP let DB do its duties.  if NOT _autocommit, then just invoke the normal commit
            self.commit()

    @staticmethod
    def _create_file(path=''):
        """ creates a file at the given path and sets the permissions to user only read/write """
        if not isfile(path):  # only do the following if the file doesn't exist yet
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
        """ store a python object in the database """
        logger.debug('storing item: %s', item)
        item_id = self._id_of(item)
        logger.debug('item_id: %s', item_id)
        if item_id is None:
            logger.debug('storing item %s', item)
            blob = self.serialize(item)
            with self._write_lock:
                self._execute(
                        'INSERT into objects (code) values (?);',
                        (blob,)
                )
                self.autocommit()

    def delete_item(self, item):
        """ removes an item from the db """
        for relation in self.relations_of(item):
            self.delete_relation(item, relation)
        for origin, relation in self.relations_to(item, True):
            self.delete_relation(origin, relation, item)
        with self._write_lock:
            self._execute('''DELETE from objects where code=?''', (self.serialize(item),))
            self.autocommit()

    def replace_item(self, old_item, new_item):
        if self._id_of(old_item) is not None:  # if there is something to replace
            if self._id_of(new_item) is None:  # if the replacement does not already exist
                with self._write_lock:
                    self._execute('''UPDATE objects set code=? where code=?''',
                            (self.serialize(new_item), self.serialize(old_item)))
                    self.autocommit()
            else:  # if the replacement does exist, just move the links from old to new
                for relation, target in self.relations_of(old_item, True):
                    self.store_relation(new_item, relation, target)
                for origin, relation in self.relations_to(old_item, True):
                    self.store_relation(origin, relation, new_item)
                self.delete_item(old_item)

    def _id_of(self, target):
        try:
            self._execute('select id from objects where code=? limit 1;', (self.serialize(target),))
            return self._fetchone()[0]
        except Exception:  #MP what exception?
            return None

    def __contains__(self, target):
        return self._id_of(target) is not None

    def __iadd__(self, target):
        """ use this to combine databases """
        assert type(target) is self.__class__, 'graph databases can only be added to other graph databases'
        for src, name, dst in target.list_relations():
            self.store_relation(src, name, dst)  #MP AWESOME!!!

    def __add__(self, target):
        """ use this to create a joined database from two graph databases """
        assert type(target) is self.__class__, 'graph databases can only be added to other graph databases'
        out = GraphDB()
        out += self
        out += target
        return out

    @staticmethod
    def __require_string__(target):
        #MP this needs to detect py2 vs py3, unicode doenst exist in py3
        #MP why checking with strings of types, not types themselves?
        #MP the potential for (subtle!) complications is too big
        assert type(target).__name__ in { 'str', 'unicode' }, 'string required'

    def store_relation(self, src, name, dst):
        """ use this to store a relation between two objects """
        self.__require_string__(name)
        logger.debug('storing relation: %s , %s , %s', src, name, dst)
        # make sure both items are stored
        self.store_item(src)
        self.store_item(dst)
        with self._write_lock:
            logger.debug(locals())
            # run the insertion
            self._execute(
                    #MP if you're jamming SQL into Python, might as well use the non-interpolated
                    #MP strings to do layout for readability
                    '''
                    insert into relations
                        select ob1.id, ?, ob2.id
                            from objects as ob1, objects as ob2
                            where ob1.code=?
                            and ob2.code=?;''',
                    (name, self.serialize(src), self.serialize(dst))
            )
            self.autocommit()  #MP i doubt that's what you mean.
            #MP if you need locking around a query and forced commit at the end, then this is
            #MP exactly what http://www.sqlitetutorial.net/sqlite-transaction/ are for.
            #MP look at these examples, they're close to what you need for DB initialization

    def _delete_single_relation(self, src, relation, dst):
        """ deletes a single relation between objects """
        self.__require_string__(relation)
        src_id = self._id_of(src)
        dst_id = self._id_of(dst)
        with self._write_lock:
            self._execute('''
                DELETE from relations where src=? and name=? and dst=?
            ''', (src_id, relation, dst_id))
            self.autocommit()  #MP convert to transaction

    def delete_relation(self, src, relation, *targets):
        """ can be both used as (src, relation, dest) for a single relation or
            (src, relation) to delete all relations of that type from the src """
        self.__require_string__(relation)
        if targets:
            for i in targets:
                self._delete_single_relation(src, relation, i)
        else:
            # delete all connections of that relation from src
            for i in list(self.find(src, relation)):
                self._delete_single_relation(src, relation, i)

    def find(self, target, relation):
        """ returns back all elements the target has a relation to """
        query = '''
            select ob1.code
                from objects as ob1, objects as ob2, relations
                where relations.dst=ob1.id
                    and relations.name=?
                    and relations.src=ob2.id
                    and ob2.code=?'''  # src is id not source :/
        for i in self._execute(query, (relation, self.serialize(target))):
            yield self.deserialize(i[0])  #MP can you deserialize inside SQLite3? might have b64

    def relations_of(self, target, include_object=False):
        """ list all relations the originate from target """
        if include_object:
            sqlresult = self._execute('''
                select relations.name, ob2.code
                    from relations, objects as ob1, objects as ob2
                    where relations.src=ob1.id
                        and ob2.id=relations.dst
                        and ob1.code=?''',
                    (self.serialize(target),))
            for i in sqlresult:
                yield i[0], self.deserialize(i[1])
        else:
            sqlresult = self._execute('''
                select distinct relations.name
                    from relations, objects
                    where relations.src=objects.id
                    and objects.code=?''',
                    (self.serialize(target),))
            for i in sqlresult:
                yield i[0]
                #MP if all you need is the first one, add `LIMIT 1` to SQL, MUCH_faster, nukes this section

    def relations_to(self, target, include_object=False):
        """ list all relations pointing at an object """
        if include_object:
            #MP subselects are painful, convert to join or where conditions if possible
            sqlresult = self._execute('''
                select name, (select code from objects where id=src) from relations where dst=?
            ''', (self._id_of(target),))
            for i in sqlresult:
                yield self.deserialize(i[1]), i[0]  #MP add 'LIMIT 1' to SQL
        else:
            sqlresult = self._execute('''select distinct name from relations where dst=?''', (self._id_of(target),))
            for i in sqlresult:
                yield i[0]  #MP add 'LIMIT 1' to SQL

    def connections_of(self, target):
        """ generate tuples containing (relation, object_that_applies) """
        #MP convert to pipeline, onelining comprehensions reads goofy
        return gen.chain(((r, i) for i in self.find(target, r)) for r in self.relations_of(target))

    def list_objects(self):
        """ list the entire of objects with their (id, serialized_form, actual_value) """
        #MP list each field, never use * cuz you dont know order or amount of stuff you get
        for i in self._execute('select * from objects'):
            _id, code = i
            yield _id, code, self.deserialize(code)

    def __iter__(self):
        """ iterate over all stored objects in the database """
        for i in self._execute('select code from objects'):
            yield self.deserialize(i[0])

    def show_objects(self):
        """ display the entirety of objects with their (id, serialized_form, actual_value) """
        for i in self.list_objects():
            print(*i)

    def list_relations(self):
        """ list every relation in the database as (src, relation, dst) """
        #MP never 'select *' in SQL
        sqlresult = self._execute('select * from relations').fetchall()
        for src, name, dst in sqlresult:
            logger.debug(src, name, dst)
            src = self.deserialize(
                    next(self._execute('select code from objects where id=?', (src,)))[0]
            )
            dst = self.deserialize(
                    next(self._execute('select code from objects where id=?', (dst,)))[0]
            )
            yield src, name, dst

    def show_relations(self):
        """ display every relation in the database as (src, relation, dst) """
        for i in self.list_relations():
            print(*i)

    def __getitem__(self, key):
        if self._autostore:
            self.store_item(key)
        return VList([V(self, key)])

    def __call__(self, key):
        return VList([V(self, key)])


class V(object):
    """docstring for V"""  #MP then write it ;)
    #MP why does this class have no public methods?
    __slots__ = ('_graph_value', '_graph_db', '_relations')

    def __init__(self, db, value):
        self._graph_db = db
        self._graph_value = value
        #MP that's not readable...WAT?
        self._relations = lambda s=self: list(s._graph_db.relations_of(s._graph_value))
        #MP wasnt there a helper function somewhere where you listed all relationships for a key?
        #MP don't forget to leverage your own helpers ;)
        #self._connections = lambda db=db, s=self: db.connections_of(s)

    def __getattribute__(self, key):
        """ this runs a query on the next step of the query """
        logger.debug(key)
        #MP which one happens more often? might be worth seeing which one runs faster the other way around
        if key in V.__slots__ or key == '__slots__':
            return object.__getattribute__(self, key)
        else:
            #MP if you're going to use a variable, name it.  if you don't, then use _
            return VList(V(self._graph_db, found) for found in self._graph_db.find(self._graph_value, key))
            #return V(self._graph_db, next(self._graph_db.find(self._graph_value, key), None))

    __getitem__ = __getattribute__

    def to(self, output_type):
        assert type(output_type) == type, 'needed a type here not: {}'.format(output_type)
        return output_type([self()])

    def __setattr__(self, key, value):
        logger.debug('%s -> %s', key, value)
        if type(value) == VList:
            # create a relation to every object in the VList
            for i in value():
                setattr(self, key, i)
        if type(value) == V:
            value = value()  # load the actual value if the target is this specific class
        if key in self.__slots__:
            object.__setattr__(self, key, value)
        else:
            self._graph_db.store_relation(self(), key, value)

    def __call__(self):
        #MP if _graph_value is the main payload of the object, then dont hide it
        #MP with _ and then re-expose with __call__ wrapper
        return self._graph_value


class VList(list):
    _slots = tuple(dir(list)) + ('_slots', 'to', 'where')

    def __init__(self, *args):
        list.__init__(self, *args)  #MP isn't this just list(*args)?
        for i in self:
            logger.debug(type(i))
            assert type(i) == V, 'needed a V and got a {}'.format(type(i))

    def where(self, *args):
        """ use this to filter VLists, simply provide a filter function and what relation to apply it to """
        assert 0 < len(args) < 3, 'invalid number of arguments for where'
        if len(args) == 1:  #MP unused params?  why?  or just unfininshed code?
            relation, filter_fn = '', args[0]
        elif len(args) == 2:
            relation, filter_fn = args

        assert callable(filter_fn), 'VList.where needs filter_fn to be a callable function'
        assert type(relation).__name__ in { 'str', 'unicode' }, 'where needs the first arg to be a string'

        def _output(self=self, relation=relation, filter_fn=filter_fn):
            """ this is an internal function because the output needs to be a VList
            but the problem is cleaner with a generator """
            #MP is VList._relations a set? membership check might be expensive
            #MP any() expr might be quicker to fail out (data dependent, so if the
            #MP proportion of which check runs quicker/cuts out more work should go
            #MP up front
            if relation:  # if a relation is defined
                for i in self:
                    if relation in i._relations() and any(filter_fn(rel()) for rel in i[relation]):
                        yield i
            else:  # if no relation is defined, check against current values
                for i in self:
                    if filter_fn(i()):
                        yield i
            #MP replace with two generator expressions?
            #MP py<3.3 wont return a generator

        return VList(_output())  #MP is this a recursive generator?  -1000 points unless very good reasons

    def to(self, output_type):
        assert type(output_type) == type, 'needed a type here not: {}'.format(output_type)
        return output_type(self())

    def __setattr__(self, key, value):
        logger.debug('%s -> %s', key, value)
        if type(value) == VList:
            # create a relation to every object in the VList
            for v in self():
                setattr(self, key, v)  #MP where does 'i' come from? should it be 'v'?
        if type(value) == V:
            value = value()  # load the actual value if the target is this specific class
        if key in self._slots:
            object.__setattr__(self, key, value)
        else:
            for v in self:
                setattr(v, key, value)
            #self._graph_db.store_relation(self(), key, value)

    def __getattribute__(self, key):
        if key in VList._slots:
            retval = object.__getattribute__(self, key)
        else:
            # run the attribute query on all elements in self
            #MP convert to pipeline
            g = lambda: gen.chain((fv for fv in getattr(v, key)) for v in self)
            retval = VList(g())
        return retval

    __getitem__ = __getattribute__

    def __call__(self, output_type=None):
        # load all values where this is called
        if output_type is None:
            retval = (i() for i in self)
        else:
            retval = output_type(i() for i in self)
        return retval


if __name__ == '__main__':
    pass
#MP this sort of stuff belongs in a Makefile or some CI/CD runner, not main
# run_tests()
# run_unittests()
# run_benchmarks()
