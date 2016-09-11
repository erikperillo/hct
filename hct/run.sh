#!/bin/bash

spark_cmd=/home/erik/downloads/rand/spark-2.0.0-bin-hadoop2.7/bin/spark-submit
file=hct.py
args="$@"

$spark_cmd --master local[4] $file $args
