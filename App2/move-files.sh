#!/bin/bash

if [[ $# -lt 2 ]]; then
    echo 'Use: ./move-files.sh <src-dir> <dest-dir> <optional : num-files>'
    exit 1
fi

SRC_DIR=$1
DEST_DIR=$2
NUM=${3:-3}

for num in `seq 1 $NUM`;
do
	mv $SRC_DIR'words_'$num'.txt' $DEST_DIR
done
