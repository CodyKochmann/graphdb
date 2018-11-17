import dill
from threading import Lock
from base64 import b64encode as b64e
from strict_functions import overload
from functools import partial

def serialize(item):
    # b64e is used on top of dumps because python loses data when encoding
    # dilled objects for sqlite
    return b64e(dill.dumps(
        item,
        protocol=dill.HIGHEST_PROTOCOL
    ))


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
    __slots__ = '_obj', '_hash', 'incoming', 'outgoing'
    def __init__(self, obj):
        self.obj = obj
        self.incoming = RelationCollection() # relations to the node
        self.outgoing = RelationCollection() # relations from the node
    @property
    def obj(self):
        return self._obj
    @obj.setter
    def obj(self, value):
        self._obj = value
        self._hash = self._rehash()
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
        if relation_name in self.outgoing:
            self.outgoing[relation_name].remove(target)
        if target in self.outgoing[relation_name]:
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
        self.obj = None
    def _rehash(self):
        #print('hashing with id')
        return hash( (type(self._obj), id(self._obj)) )
    @overload
    def _rehash(self):
        #print('hashing with object')
        return hash( (type(self._obj), hash(self._obj)) )

class RamGraphDB(object):
    ''' sqlite based graph database for storing native python objects and their relationships to each other '''

    def __init__(self, autostore=True):
        self.nodes = {} # stores node_hash:node
        self._autostore = autostore
        self._write_lock = Lock()

    def _destroy(self):
        raise NotImplementedError()

    @staticmethod
    def _item_hash(item):
        return hash(item if isinstance(item, RamGraphDBNode) else RamGraphDBNode(item))

    def __contains__(self, item):
        return self._item_hash(item) in self.nodes

    def _get_item_node(self, item):
        assert item in self
        return self.nodes[self._item_hash(item)]

    def store_item(self, item):
        ''' use this function to store a python object in the database '''
        assert not isinstance(item, RamGraphDBNode)
        node = RamGraphDBNode(item)
        node_hash = hash(node)
        if node not in self:
            self.nodes[node_hash] = node
        return self.nodes[node_hash]

    def replace_item(self, old_item, new_item):
        new_hash = self._item_hash(new_item)
        old_hash = self._item_hash(old_item)
        assert old_hash in self.nodes
        old_node = self.nodes[old_hash]
        if new_hash in self.nodes:
            self.nodes[new_hash].absorb(old_node)
        else:
            old_node.obj = new_item
            self.nodes[new_hash] = old_node
        del self.nodes[old_hash]

        self.nodes[self._item_hash(old_item)]
        raise NotImplementedError()
        # delete replace the object so it applies to all established relations

    def _id_of(self, target):
        raise NotImplementedError()


    def __iadd__(self, target):
        ''' use this to combine databases '''
        raise NotImplementedError()
        assert type(target) is self.__class__, 'graph databases can only be added to other graph databases'
        #for src, name, dst in target.list_relations():
        #    self.store_relation(src, name, dst)

    def __add__(self, target):
        ''' use this to create a joined database from two graph databases '''
        assert type(target) is self.__class__, 'graph databases can only be added to other graph databases'
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
        print('storing relation', src, name, dst)
        # make sure both items are stored
        self.store_item(src).link(name, self.store_item(dst))


    def _delete_single_relation(self, src, relation, dst):
        ''' deletes a single relation between objects '''
        raise NotImplementedError()
        self.__require_string__(relation)

    def delete_relation(self, src, relation, *targets):
        ''' can be both used as (src, relation, dest) for a single relation or
            (src, relation) to delete all relations of that type from the src '''
        self.__require_string__(relation)
        src_node = self._get_item_node(src)
        for t in targets:
            src_node.unlink(relation, self._get_item_node(t))

    def delete_item(self, item):
        ''' removes an item from the db '''
        for relation, dst in self.relations_of(item, True):
            self.delete_relation(item, relation, dst)
            #print(item, relation, dst)
        for relation, src in self.relations_to(item, True):
            self.delete_relation(src, relation, item)
            #print(src, relation, item)
        h = self._item_hash(item)
        if item in self:
            print('deleting item:', item)
            self.nodes[h].clear()
            del self.nodes[h]

    def find(self, target, relation):
        ''' returns back all elements the target has a relation to '''
        return self._get_item_node(target).outgoing[relation]

    def relations_of(self, target, include_object=False):
        ''' list all relations the originate from target '''
        relations = self._get_item_node(target).outgoing
        if include_object:
            for k in relations:
                for v in relations[k]:
                    yield k, v.obj
        else:
            yield from relations

    def relations_to(self, target, include_object=False):
        ''' list all relations pointing at an object '''
        relations = self._get_item_node(target).incoming
        if include_object:
            for k in relations:
                for v in relations[k]:
                    yield k, v.obj
        else:
            yield from relations

    def connections_of(self, target):
        ''' generate tuples containing (relation, object_that_applies) '''
        raise NotImplementedError()
        #return gen.chain( ((r,i) for i in self.find(target,r)) for r in self.relations_of(target) )

    def iter_nodes(self):
        for _id in self.nodes:
            yield self.nodes[_id]

    def __iter__(self):
        ''' iterate over all stored objects in the database '''
        for node in self.iter_nodes():
            yield node.obj
        #for i in self._execute('select code from objects'):
        #    yield self.deserialize(i[0])

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
        raise NotImplementedError()
        #if self._autostore:
        #    self.store_item(key)
        #return VList([V(self, key)])

    def __call__(self, key):
        raise NotImplementedError()
        #return VList([V(self, key)])


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
    assert list(db.relations_to('bob', True)) == [('knows', 'tom')]
    assert isinstance(db.find('tom', 'knows'), NodeCollection)
    assert {i.obj for i in db.find('tom', 'knows')} == {'bob', 'bill'}
    db.delete_relation('tom', 'knows', 'bill')
    show()
    assert set(db.relations_of('tom', True)) == {('knows', 'bob')}
    assert set(db.list_relations()) == {('tom', 'knows', 'bob')}
    db.delete_item('bob')
    show()
    assert 'bob' not in db

    exit()

if __name__ == '__main__':
    import __test__
    __test__.GraphDB = RamGraphDB
    GraphDBTest = __test__.GraphDBTest
    __test__.unittest.main()
