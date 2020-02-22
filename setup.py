from distutils.core import setup
import sys, os, setuptools

version = '2020.2.4'
name = 'graphdb'
description = 'sqlite based graph database for storing native python objects and their relationships to each other',
keywords = ['graphdb', 'graph', 'sqlite', 'database', 'db', 'node', 'relation', 'attribute'],

packages = setuptools.find_packages()

assert name in packages, [name, packages]  # if package name doesnt show up, something is wrong

def using_ios_stash():
    ''' returns true if sys path hints the install is running on ios '''
    print('detected install path:')
    print(os.path.dirname(__file__))
    module_names = set(sys.modules.keys())
    return 'stash' in module_names or 'stash.system' in module_names

def requires():
    ''' generates the package requirements live based on system configuration '''
    yield "generators"
    yield "strict_functions"
    if not using_ios_stash():
        yield "dill"

setup(
  name = name,
  version = version,
  packages = packages,
  install_requires = list(requires()),
  zip_safe=True,
  description = description,
  author = 'Cody Kochmann',
  author_email = 'kochmanncody@gmail.com',
  url = 'https://github.com/CodyKochmann/{}'.format(name),
  download_url = 'https://github.com/CodyKochmann/{}/tarball/{}'.format(name, version),
  keywords = keywords,
  classifiers = []
)
