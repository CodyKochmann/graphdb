import sys

from graphdb import GraphDB, RamGraphDB, SQLiteGraphDB

from .generate_tests import generate_api_tests

__all__ = ['TestGraphDB', 'TestSQLiteGraphDB']

TestGraphDB       = generate_api_tests(GraphDB)
TestSQLiteGraphDB = generate_api_tests(SQLiteGraphDB)

if sys.version_info > (3, 5):
	TestRamGraphDB    = generate_api_tests(RamGraphDB)
	__all__.append('TestRamGraphDB')
	