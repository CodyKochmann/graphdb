#!/bin/bash

### All the settings are in teh .pylintrc, tweak it there, 
### not on the commandline invocation
pylint --rcfile=linters/.pylintrc \
    graphdb/*.py
