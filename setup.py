from distutils.core import setup
setup(
  name = 'graphdb',
  packages = ['graphdb'], # this must be the same as the name above
  version = '2017.11.9',
  install_requires = ["generators", "strict_functions"],
  description = 'sqlite based graph database for storing native python objects and their relationships to each other',
  author = 'Cody Kochmann',
  author_email = 'kochmanncody@gmail.com',
  url = 'https://github.com/CodyKochmann/graphdb',
  download_url = 'https://github.com/CodyKochmann/graphdb/tarball/2017.11.9',
  keywords = ['graphdb', 'graph', 'sqlite', 'database', 'db', 'node', 'relation', 'attribute'],
  classifiers = [],
)
