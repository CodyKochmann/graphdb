import sys

if sys.version_info < (3,0):
    raise SystemError('RamGraphDB does not support python 2, TIME TO UPGRADE!!! :D')

from threading import Lock
from base64 import b64encode as b64e
from strict_functions import overload
import generators as gen
from functools import partial


def graph_hash(obj):
    '''this hashes all types to a hash without colissions. python's hashing algorithms are not cross type compatable but hashing tuples with the type as the first element seems to do the trick'''
    obj_type = type(obj)
    try:
        # this works for hashables
        return hash((obj_type, obj))
    except:
        # this works for object containers since graphdb
        # wants to identify different containers
        # instead of the sum of their current internals
        return hash((obj_type, id(obj)))

class better_default_dict(dict):
    def __init__(self, constructor):
        if not callable(constructor):
            constructor = lambda:constructor
        self._constructor = constructor

    def __getitem__(self, target):
        if target not in self:
            dict.__setitem__(self, target, self._constructor())
        return dict.__getitem__(self, target)

class NodeLinker(dict):
    """linker between nodes in RamGraphDB"""
    def __init__(self):
        dict.__init__(self)
    def __dir__(self):
        return dir({}) + self.keys()

class NodeCollection(list):
    def __init__(self, owner=None):
        list.__init__(self)
        self.owner = owner
    def clear(self):
        list.clear(self)
        self.owner.autoclean()
    def pop(self, *args, **kwargs):
        list.pop(self, *args, **kwargs)
        if not self:
            self.owner.autoclean()
    def append(self, new_node):
        assert isinstance(new_node, RamGraphDBNode), 'NodeCollections can only append RamGraphDBNodes'
        list.append(self, new_node)
    def __iadd__(self, target):
        for t in target:
            self.append(t)
    @overload
    def __iadd__(self, target):
        self.append(target)

class RelationCollection(better_default_dict):
    def __init__(self):
        better_default_dict.__init__(self, partial(NodeCollection, self))
    def __iadd__(self, target):
        assert isinstance(target, RelationCollection)
        for key in target:
            self[key] += target[key]

    def autoclean(self):
        targets = {k for k, v in self.items() if not v}
        for t in targets:
            del self[t]

    def __add__(self, target):
        out = RelationCollection()
        out += self
        out += target
        return out
    def clear(self):
        for key in list(self):
            self[key].clear()
        dict.clear(self)

class RamGraphDBNode(object):
    """object containers for RamGraphDB to store objects in"""
    __slots__ = 'obj', '_hash', 'incoming', 'outgoing'
    def __init__(self, obj):
        self.obj = obj
        self._hash = graph_hash(obj)
        self.incoming = RelationCollection() # relations to the node
        self.outgoing = RelationCollection() # relations from the node

    def __hash__(self):
        return self._hash

    @staticmethod
    def __validate_relation_name__(relation_name):
        assert isinstance(relation_name, str) and relation_name, 'relation_names have to be non-empty strings'
    @staticmethod
    def __validate_link_target__(target):
        assert isinstance(target, RamGraphDBNode), 'RamGraphDBNodes can only link to other RamGraphDBNodes'

    def link(self, relation_name, target):
        self.__validate_relation_name__(relation_name)
        self.__validate_link_target__(target)
        if target not in self.outgoing[relation_name]:
            self.outgoing[relation_name].append(target)
        if self not in target.incoming[relation_name]:
            target.incoming[relation_name].append(self)
    def unlink(self, relation_name, target):
        self.__validate_relation_name__(relation_name)
        self.__validate_link_target__(target)
        if relation_name in self.outgoing and target in self.outgoing[relation_name]:
            self.outgoing[relation_name].remove(target)
            target.incoming[relation_name].remove(self)
    def __eq__(self, target):
        return target.obj == self.obj or target.obj is self.obj
    def absorb(self, target):
        assert self == target, 'can only absorb nodes with the same internal obj'
        self.incoming += target.incoming
        self.outgoing += target.outgoing
        target.clear()
    def clear(self):
        self.incoming.clear()
        self.outgoing.clear()
        del self.obj

class RamGraphDB(object):
    ''' sqlite based graph database for storing native python objects and their relationships to each other '''

    def __init__(self, autostore=True):
        self.nodes = {} # stores node_hash:node
        self._autostore = autostore
        self._write_lock = Lock()

    def _destroy(self):
        targets = list(self)
        for t in targets:
            self.delete_item(t)

    @staticmethod
    def _item_hash(item):
        #assert not isinstance(item, RamGraphDBNode)
        return item._hash if isinstance(item, RamGraphDBNode) else graph_hash(item)

    def __contains__(self, item):
        return self._item_hash(item) in self.nodes

    def _get_item_node(self, item):
        return item if isinstance(item, RamGraphDBNode) else self.nodes[self._item_hash(item)]

    def store_item(self, item):
        ''' use this function to store a python object in the database '''
        assert not isinstance(item, RamGraphDBNode)
        item_hash = graph_hash(item)
        if item_hash not in self.nodes:
            self.nodes[item_hash] = RamGraphDBNode(item)
        return self.nodes[item_hash]

    def replace_item(self, old_item, new_item):
        for relation, dst in self.relations_of(old_item, True):
            self.delete_relation(old_item, relation, dst)
            self.store_relation(new_item, relation, dst)
        for src, relation in self.relations_to(old_item, True):
            self.delete_relation(src, relation, old_item)
            self.store_relation(src, relation, new_item)
        self.delete_item(old_item)

    _id_of = _item_hash

    @staticmethod
    def serialize(o):
        '''this is a placeholder function to support SQLiteGraphDB api compatibility. NO SERIALIZING IN RAM!!!'''
        return o

    @staticmethod
    def deserialize(o):
        '''this is a placeholder function to support SQLiteGraphDB api compatibility. NO SERIALIZING IN RAM!!!'''
        return o

    def __iadd__(self, target):
        ''' use this to combine databases '''
        assert isinstance(target, RamGraphDB), 'graph databases can only be added to other graph databases'
        for src, name, dst in target.list_relations():
            self.store_relation(src, name, dst)
        return self

    def __add__(self, target):
        ''' use this to create a joined database from two graph databases '''
        assert isinstance(target, RamGraphDB), 'graph databases can only be added to other graph databases'
        out = RamGraphDB()
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
        self.store_item(src).link(name, self.store_item(dst))


    def _delete_single_relation(self, src, relation, dst):
        ''' deletes a single relation between objects '''
        raise NotImplementedError()
        self.__require_string__(relation)

    def delete_relation(self, src, relation, target):
        ''' can be both used as (src, relation, dest) for a single relation or
            (src, relation) to delete all relations of that type from the src '''
        self.__require_string__(relation)
        if src in self and target in self:
            self._get_item_node(src).unlink(relation, self._get_item_node(target))

    def delete_item(self, item):
        ''' removes an item from the db '''
        for relation, dst in self.relations_of(item, True):
            self.delete_relation(item, relation, dst)
            #print(item, relation, dst)
        for src, relation in self.relations_to(item, True):
            self.delete_relation(src, relation, item)
            #print(src, relation, item)
        h = self._item_hash(item)
        if item in self:
            #print('deleting item:', item)
            self.nodes[h].clear()
            del self.nodes[h]

    def find(self, target, relation):
        ''' returns back all elements the target has a relation to '''
        return self._get_item_node(target).outgoing[relation]

    def relations_of(self, target, include_object=False):
        ''' list all relations the originate from target '''
        relations = (target if isinstance(target, RamGraphDBNode) else self._get_item_node(target)).outgoing
        if include_object:
            for k in relations:
                for v in relations[k]:
                    if hasattr(v, 'obj'): # filter dead links
                        yield k, v.obj
        else:
            yield from relations

    def relations_to(self, target, include_object=False):
        ''' list all relations pointing at an object '''
        relations = self._get_item_node(target).incoming
        if include_object:
            for k in relations:
                for v in relations[k]:
                    if hasattr(v, 'obj'): # filter dead links
                        yield v.obj, k
        else:
            yield from relations

    def iter_nodes(self):
        for _id in self.nodes:
            yield self.nodes[_id]

    def __iter__(self):
        ''' iterate over all stored objects in the database '''
        for node in self.iter_nodes():
            yield node.obj
        #for i in self._execute('select code from objects'):
        #    yield self.deserialize(i[0])

    list_objects = __iter__

    def show_objects(self):
        ''' display the entire of objects with their (id, value, node) '''
        for key in self.nodes:
            node = self.nodes[key]
            value = node.obj
            print(key, '-', repr(value), '-', node)

    def list_relations(self):
        ''' list every relation in the database as (src, relation, dst) '''
        for node in self.iter_nodes():
            for relation, target in self.relations_of(node.obj, True):
                yield node.obj, relation, target

    def show_relations(self):
        ''' display every relation in the database as (src, relation, dst) '''
        for src_node in self.iter_nodes():
            for relation in src_node.outgoing:
                for dst_node in src_node.outgoing[relation]:
                    print(repr(src_node.obj), '-', relation, '-', repr(dst_node.obj))

    def __getitem__(self, key):
        if self._autostore:
            self.store_item(key)
        return VList([V(self, key)])

    def __call__(self, key):
        return VList([V(self, key)])


class V(object):
    """docstring for V"""
    __slots__ = {'_graph_value','_graph_db','_relations'}
    __reserved__ = __slots__.union({'__slots__'})

    def __init__(self, db, value):
        self._graph_db = db
        self._graph_value = value
        self._relations = lambda s=self: list(s._graph_db.relations_of(s._graph_value))
        #self._connections = lambda db=db, s=self: db.connections_of(s)

    def __getattribute__(self, key):
        ''' this runs a query on the next step of the query '''
        return object.__getattribute__(self, key) if key in V.__reserved__ else VList(V(self._graph_db, _) for _ in self._graph_db.find(self._graph_value, key))

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
        return self._graph_value.obj if isinstance(self._graph_value, RamGraphDBNode) else self._graph_value

class VList(list):
    _slots = tuple(dir(list)) + ('_slots','to','where')

    #def __init__(self, arg):
    #    list.__init__(self, arg)
    #    assert all(type(i)==V for i in self), 'VLists can only contain V objects'

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
 

if __name__ == '__main__':
    db = RamGraphDB()
    def show():
        print('objects')
        db.show_objects()
        print('relations')
        db.show_relations()
    db.store_item('tom')
    show()
    assert 'tom' in db
    assert list(db) == ['tom']
    db.store_item('bob')
    show()
    assert 'bob' in db
    assert set(db) == {'tom', 'bob'}
    db.store_relation('tom', 'knows', 'bob')
    show()
    db.store_relation('tom', 'knows', 'bill')
    show()
    assert list(db.relations_of('tom')) == ['knows']
    assert set(db.relations_of('tom', True)) == {('knows', 'bob'), ('knows', 'bill')}
    assert list(db.relations_to('bob')) == ['knows']
    assert list(db.relations_to('bob', True)) == [('tom', 'knows')]
    assert isinstance(db.find('tom', 'knows'), NodeCollection)
    assert {i.obj for i in db.find('tom', 'knows')} == {'bob', 'bill'}
    db.delete_relation('tom', 'knows', 'bill')
    show()
    assert set(db.relations_of('tom', True)) == {('knows', 'bob')}
    assert set(db.list_relations()) == {('tom', 'knows', 'bob')}
    db.delete_item('bob')
    show()
    assert 'bob' not in db
    assert set(db.list_relations()) == set()

    db.store_relation('abby', 'knows', 'tom')
    show()
    db.replace_item('tom', 'cody')
    assert set(db.list_relations()) == {('abby', 'knows', 'cody')}
    show()

    db._destroy()
    show()
    assert len(list(db.list_relations())) == 0
    assert len(list(db)) == 0

    db1 = RamGraphDB()
    db2 = RamGraphDB()
    db1.store_relation('bill', 'knows', 'tim')
    db2.store_relation('bill', 'knows', 'tom')
    assert set((db1 + db2).list_relations()) == {('bill', 'knows', 'tim'), ('bill', 'knows', 'tom')}

    db1._destroy()
    db2._destroy()

    print('-')

    db = RamGraphDB()

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
    print(db[5].precedes.precedes.prime.to(list))
    seven.prime = True

    for i in range(1,5):
        print(i)
        db[5].greater_than = i

    print(db[5].greater_than.to(list))

    db.show_objects()
    db.show_relations()
    print(list(db.relations_of(5)))

    print()

    print(list(gen.chain( ((r,i.obj) for i in db.find(5,r)) for r in db.relations_of(5) )))

    for r in db.relations_of(5):
        print(r)
        print([i.obj for i in db.find(5,r)])

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


    exit()

if __name__ == '__main__':
    import __test__
    __test__.GraphDB = RamGraphDB
    GraphDBTest = __test__.GraphDBTest
    __test__.unittest.main()
