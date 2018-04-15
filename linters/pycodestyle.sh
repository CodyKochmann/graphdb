#!/bin/bash

function pycodestyle_standalone() {
    pycodestyle --show-source \
        --format=pylint \
        --ignore=E265,E262,E501,E128,E126,E201,E202,E501 \
        graphdb/
}


function pycodestyle_machine_friendly() {
    pycodestyle \
        --ignore=E265,E262,E501,E128,E126,E201,E202,E501 \
        graphdb/
}

pycodestyle_machine_friendly
