#!/bin/bash
# author TurboChang

py_3=$(which python3)
runner=runner.py

while [ 1 ]
  p_ops=$1
  p_batch=$2
  p_time=$3
  do
    if [ $p_ops != "compare" ];
    then
      $py_3 $runner --ops $p_ops --db Oracle --batch $p_batch
    else
      $py_3 $runner --ops $p_ops
    fi
    sleep $p_time
  done