#!/bin/bash

function pytest_with_coverage() {
    pytest -v \
        --cov-report=term-missing \
        --cov=. \
        --cov-branch \
        tests/test_graphdb.py
}

function pytest_without_coverage() {
    pytest -v \
        tests
}

pytest_without_coverage
