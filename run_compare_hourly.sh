#!/bin/bash

PYTHON_EXE=$1
PYTHON_SCRIPT=$2
DOWNLOAD_DIR=$3
REPORT_DIR=$4
DATASET=$5
START_DATE=$(TZ=America/Los_Angeles date -d '1 hour ago' +"%Y-%m-%dT%H:%M:%S")
END_DATE=$(TZ=America/Los_Angeles date +"%Y-%m-%dT%H:%M:%S")

echo "$PYTHON_EXE $PYTHON_SCRIPT -s $START_DATE -e $END_DATE -c $DATASET -d -o $DOWNLOAD_DIR -r $REPORT_DIR -t -H"
$PYTHON_EXE $PYTHON_SCRIPT -s $START_DATE -e $END_DATE -c $DATASET -d -o $DOWNLOAD_DIR -r $REPORT_DIR -t -H

exit 0