#!/bin/bash

PYTHON_EXE=$1
PYTHON_SCRIPT=$2
DOWNLOAD_DIR=$3
REPORT_DIR=$4
DATASET=$5
AWS_CREDS=$6
LOG_DIR=$7
HTML_DIR=$8

START_DATE=$(date -d '1 hour ago' +"%Y-%m-%dT%H:%M:%S")
END_DATE=$(date +"%Y-%m-%dT%H:%M:%S")

source $AWS_CREDS

echo "$PYTHON_EXE $PYTHON_SCRIPT -s $START_DATE -e $END_DATE -c $DATASET -d -o $DOWNLOAD_DIR -r $REPORT_DIR -t -l $LOG_DIR -w -p $HTML_DIR --revision"
$PYTHON_EXE $PYTHON_SCRIPT -s $START_DATE -e $END_DATE -c $DATASET -d -o $DOWNLOAD_DIR -r $REPORT_DIR -t -l $LOG_DIR -w -p $HTML_DIR --revision

exit 0