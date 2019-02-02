# graphdb

[![Downloads](https://pepy.tech/badge/graphdb)](https://pepy.tech/project/graphdb)
[![Downloads](https://pepy.tech/badge/graphdb/month)](https://pepy.tech/project/graphdb)
[![Downloads](https://pepy.tech/badge/graphdb/week)](https://pepy.tech/project/graphdb)

A sqlite based graph database for storing native python objects and their relationships to each other.

![ ](https://bit.ly/graph_db_png)

## How to install it?

```
pip install graphdb
```

## How to use it?

```python
In [1]: # import GraphDB

In [2]: from graphdb import GraphDB

In [3]: # initialize a database

In [4]: db = GraphDB('/tmp/test_graph.db') # uses ':memory:' if no path

In [5]: # you can store relations between native python objects

In [6]: for a in range(10):
   ...:     b = a + 1
   ...:     print(a, b)
   ...:     db.store_relation(a, 'comes_before', b)
   ...:
0 1
1 2
2 3
3 4
4 5
5 6
6 7
7 8
8 9
9 10

In [7]: # you have as many relationships between objects as you want

In [8]: def is_prime(a):
   ...:     ''' returns true if 'a' is prime '''
   ...:     return a>1 and all(a % i for i in range(2, a))
   ...:
   ...: def preceding_primes(a):
   ...:     ''' returns a list of prime numbers less than 'a' '''
   ...:     return [i for i in range(a) if is_prime(i)]
   ...:
   ...: for i in range(11):
   ...:     for p in preceding_primes(i):
   ...:         db.store_relation(i, 'preceding_prime', p)
   ...:

In [9]: # queries to the database are done through attribute chains

In [10]: db(7).comes_before.preceding_prime(list)
Out[10]: [2, 3, 5, 7]

In [11]: # to break the query into pieces, we start with the starting point

In [12]: db(7)(list) # tells the db to start the query at this object
Out[12]: [7]

In [13]: # now we add 'comes_before' to hop to whatever object '7 comes_before'

In [14]: db(7).comes_before(list)
Out[14]: [8]

In [15]: # then we add the 'preceding_prime' to get all connected primes to 8

In [16]: db(7).comes_before.preceding_prime(list)
Out[16]: [2, 3, 5, 7]

In [17]: # since every query outputs iterable elements, it doesn't matter how
    ...: # many elements you're currently at because elements only stay as long
    ...: # as the query applies to them

In [18]: db(7).comes_before.preceding_prime.comes_before(list)
Out[18]: [3, 4, 6, 8]

In [19]: db(7).comes_before.preceding_prime.comes_before.comes_before(list)
Out[19]: [4, 5, 7, 9]

In [20]: db(7).comes_before.preceding_prime.comes_before.comes_before.comes_before(list)
Out[20]: [5, 6, 8, 10]

In [21]: # so what if we add one more 'comes_before' even though we never
    ...: # assigned '10' a relationship to refer to?

In [22]: db(7).comes_before.preceding_prime.comes_before.comes_before.comes_before.comes_before(list)
Out[22]: [6, 7, 9]

In [23]: # the query simply drops that node since its relational path didn't
    ...: # apply to the 4th result

In [24]: # Why does every query end with '(list)'?

In [25]: # graphdb sets up queries so you can continue to get each next step
    ...: # until you add '()' to the end of it which then tells graphdb that
    ...: # you want a materialized view of that spot.
    ...:
    ...: # The reason why in this demo '(list)' was being added instead of
    ...: # '()' was because by default, graphdb returns a native python
    ...: # generator so you can iterate through each individual object

In [26]: # just to demonstrate normal behavior
    ...: db(7).comes_before()
Out[26]: <generator object VList.__call__.<locals>.<genexpr> at 0x103336f68>

In [27]: # so we could technically do
    ...: list(db(7).comes_before())
Out[27]: [8]

In [28]: # or the more natural feeling

In [29]: db(7).comes_before(list)
Out[29]: [8]

In [30]: # 'list' isn't the only type you can pass into the query breakpoint,
    ...: # any container-like type you want to use should work

In [31]: db(7).comes_before(set)
Out[31]: {8}

In [32]: # Are there any filtering mechanisms that can be used to cut down how
    ...: # many relations graphdb queries produce?

In [33]: # '.where()' is used to filter relationships. So, for example:

In [34]: # what we're gonna filter
    ...: db(8).preceding_prime(list)
Out[34]: [2, 3, 5, 7]

In [35]: # this filters preceding_primes of 8 which are larger than 5
    ...: db(8).preceding_prime.where(lambda i:i>5)(list)
Out[35]: [7]

In [36]: # or we can filter items based on their linked objects
    ...: db(8).preceding_prime.where('comes_before', lambda i:i>5)(list)
Out[37]: [5, 7]

```
