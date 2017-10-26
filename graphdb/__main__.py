# -*- coding: utf-8 -*-
# @Author: Cody Kochmann
# @Date:   2017-10-26 12:52:07
# @Last Modified 2017-10-26
# @Last Modified time: 2017-10-26 12:55:29

from sys import argv
import graphdb
import argparse

parser = argparse.ArgumentParser(prog='__main__.py')

parser.add_argument(
    '--test',
    help="run tests to see if battle_tested works correctly on you system",
    action='store_true'
)

if '__main__.py' in argv[-1] or 'help' in argv:
    parsed = parser.parse_args(['-h'])

args, unknown = parser.parse_known_args()

if args.test:
    print('-'*80)
    print('running graphdb.run_tests')
    graphdb.run_tests()
    print('-'*80)
    print('all tests were successful')
    print('-'*80)
