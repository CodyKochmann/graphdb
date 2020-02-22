from graphdb import GraphDB, RamGraphDB, SQLiteGraphDB

from .generate_tests import generate_api_tests

__all__ = ['TestGraphDB', 'TestRamGraphDB', 'TestSQLiteGraphDB']

TestGraphDB       = generate_api_tests(GraphDB)
TestRamGraphDB    = generate_api_tests(RamGraphDB)
TestSQLiteGraphDB = generate_api_tests(SQLiteGraphDB)
