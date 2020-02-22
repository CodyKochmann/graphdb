from graphdb import GraphDB, RamGraphDB, SQLiteGraphDB

from .generate_tests import generate_api_tests

TestGraphDB       = generate_api_tests(GraphDB)
TestRamGraphDB    = generate_api_tests(RamGraphDB)
TestSQLiteGraphDB = generate_api_tests(TestSQLiteGraphDB)
