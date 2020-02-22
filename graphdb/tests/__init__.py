import sys

from graphdb import GraphDB, RamGraphDB, SQLiteGraphDB

from .generate_tests import generate_api_tests

__all__ = ['TestGraphDB', 'TestSQLiteGraphDB']

TestGraphDB       = generate_api_tests(GraphDB)
TestSQLiteGraphDB = generate_api_tests(SQLiteGraphDB)

if sys.version_info <= (3, 6):
	__all__.append('TestRamGraphDB')
	TestRamGraphDB    = generate_api_tests(RamGraphDB)
