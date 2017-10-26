from distutils.core import setup
setup(
  name = 'graphdb',
  packages = ['graphdb'], # this must be the same as the name above
  version = '2017.10.26',
  install_requires = ["generators"],
  description = 'sqlite based graph database for storing native python objects and their relationships to each other',
  author = 'Cody Kochmann',
  author_email = 'kochmanncody@gmail.com',
  url = 'https://github.com/CodyKochmann/graphdb',
  download_url = 'https://github.com/CodyKochmann/graphdb/tarball/2017.10.26',
  keywords = ['graphdb', 'graph', 'sqlite', 'database', 'db', 'node', 'relation', 'attribute'],
  classifiers = [],
)
