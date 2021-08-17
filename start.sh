#!/bin/bash
# author TurboChang

py_3=$(which python3)
workdir=$(cd $(dirname $0); pwd)
runner=runner.py

log(){
	tee -a $workdir/core/report/start.log
}

while [ 1 ]
  p_ops=$1
  p_batch=$2
  p_time=$3
  do
    if [ $p_ops != "compare" ];
    then
      $py_3 $runner --ops $p_ops --db Oracle --batch $p_batch | log
    else
      $py_3 $runner --ops $p_ops | log
      $py_3 core/mail/send_mail.py
    fi
    sleep $p_time
  done