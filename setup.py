from distutils.core import setup

version = '2018.4.15'

setup(
  name = 'graphdb',
  packages = ['graphdb'], # this must be the same as the name above
  version = version,
  install_requires = ["generators", "dill"],
  description = 'sqlite based graph database for storing native python objects and their relationships to each other',
  author = 'Cody Kochmann',
  author_email = 'kochmanncody@gmail.com',
  url = 'https://github.com/CodyKochmann/graphdb',
  download_url = 'https://github.com/CodyKochmann/generators/tarball/{}'.format(version),
  keywords = ['graphdb', 'graph', 'sqlite', 'database', 'db', 'node', 'relation', 'attribute'],
  classifiers = [],
)
