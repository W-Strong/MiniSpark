#!/bin/bash

# clean up
make clean

# Check if slipdays.txt exists in the current directory
if [ -f slipdays.txt ]; then
    echo "slipdays.txt found. Including it in the archive."
    tar -zcf p5.tar ./solution README.md Makefile slipdays.txt
else
    echo "slipdays.txt not found. Proceeding without it."
    tar -zcf p5.tar ./solution README.md Makefile
fi

