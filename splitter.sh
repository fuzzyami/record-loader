# ./splitter.sh edges.json 500000000 edges edges ;50mb each file
#!/bin/bash
local_path=$1 # path to input
file_size=$2    # size, in bytes, of resulting files
root_part=$3  # base name of the output file to which “-partXXXXX” would be added
ext_part=$4

set -o pipefail
split  --line-bytes=$file_size -d --suffix-length=3 $local_path $root_part- --filter "gzip > /tmp/\$FILE && gsutil -q mv /tmp/\$FILE gs://cyco-jesse-tmp/record-loader/\$FILE\$ext_part.gz"