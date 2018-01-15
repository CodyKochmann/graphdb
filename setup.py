from distutils.core import setup

setup(
  name = 'graphdb',
  packages = ['graphdb'], # this must be the same as the name above
  version = '2018.1.18',
  install_requires = ["generators", "dill"],
  description = 'sqlite based graph database for storing native python objects and their relationships to each other',
  author = 'Cody Kochmann',
  author_email = 'kochmanncody@gmail.com',
  url = 'https://github.com/CodyKochmann/graphdb',
  download_url = 'https://github.com/CodyKochmann/generators/tarball/2018.1.18',
  keywords = ['graphdb', 'graph', 'sqlite', 'database', 'db', 'node', 'relation', 'attribute'],
  classifiers = [],
)
