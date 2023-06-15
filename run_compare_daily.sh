#!/bin/bash

PYTHON_EXE=$1
PYTHON_SCRIPT=$2
REPORT_DIR=$3
DATASET=$4
AWS_CREDS=$5
LOG_DIR=$6
MONTH=$7
START=$8
END=$9
REVISION=${10}

if [ $# -gt 10 ]; then
    HTML_DIR=${11}
else
    HTML_DIR="FALSE"
fi

for i in  $(seq -f "%02g" $START $END ) 
do

    echo "day $i"

    START_DATE="2023-$MONTH-${i}T00:00:00"
    END_DATE="2023-$MONTH-${i}T23:59:59"

    if [ "$REVISION" = "TRUE" ]; then

        if [ "$HTML_DIR" == "FALSE" ]; then

            echo "$PYTHON_EXE $PYTHON_SCRIPT -s $START_DATE -e $END_DATE -c $DATASET -r $REPORT_DIR -t -l $LOG_DIR --revision"
            $PYTHON_EXE $PYTHON_SCRIPT -s $START_DATE -e $END_DATE -c $DATASET -r $REPORT_DIR -t -l $LOG_DIR --revision

        else

            echo "$PYTHON_EXE $PYTHON_SCRIPT -s $START_DATE -e $END_DATE -c $DATASET -r $REPORT_DIR -t -l $LOG_DIR -w -p $HTML_DIR --revision"
            $PYTHON_EXE $PYTHON_SCRIPT -s $START_DATE -e $END_DATE -c $DATASET -r $REPORT_DIR -t -l $LOG_DIR -w -p $HTML_DIR --revision

        fi
        
    
    else

        if [ "$HTML_DIR" == "FALSE" ]; then

            echo "$PYTHON_EXE $PYTHON_SCRIPT -s $START_DATE -e $END_DATE -c $DATASET -r $REPORT_DIR -t -l $LOG_DIR"
            $PYTHON_EXE $PYTHON_SCRIPT -s $START_DATE -e $END_DATE -c $DATASET -r $REPORT_DIR -t -l $LOG_DIR

        else

            echo "$PYTHON_EXE $PYTHON_SCRIPT -s $START_DATE -e $END_DATE -c $DATASET -r $REPORT_DIR -t -l $LOG_DIR -w -p $HTML_DIR"
            $PYTHON_EXE $PYTHON_SCRIPT -s $START_DATE -e $END_DATE -c $DATASET -r $REPORT_DIR -t -l $LOG_DIR -w -p $HTML_DIR

        fi

    fi
    
    sleep 3
 
done
exit 0