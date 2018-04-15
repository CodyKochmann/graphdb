#!/bin/bash

coverage run -- tests/test_graphdb.py
coverage report -m graphdb/*.py

