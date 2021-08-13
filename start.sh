#!/bin/bash
# author TurboChang

while [ 1 ]
  p_ops=$1
  p_batch=$2
  p_time=$3
  do
    if [ $p_ops != "compare" ];
    then
      /usr/local/bin/python3 /data/rac_test-main/runner.py --ops $p_ops --db Oracle --batch $p_batch
    else
      /usr/local/bin/python3 /data/rac_test-main/runner.py --ops $p_ops
    fi
    sleep $p_time
  done