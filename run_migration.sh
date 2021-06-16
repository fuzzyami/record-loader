#!/bin/bash
record_type=$1
start_index=$2 
stop_index=$3
max_jobs=$4
set -eo pipefail

seq -f "%03g" $start_index $stop_index | parallel --jobs $max_jobs --delay 15 java -Xmx512M -Xms256M -jar record-loader-0.1.0-standalone.jar $record_type &
