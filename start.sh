#!/bin/bash
while [ 1 ]
do

read -t 60
/Library/Frameworks/Python.framework/Versions/3.9/bin/python3 /Users/changliuxin/Programs/datapipeline/rac_test/runner.py --ops insert --db Oracle --batch 10000
done
