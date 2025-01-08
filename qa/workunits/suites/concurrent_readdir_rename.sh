#!/usr/bin/env bash

set -ex

mkdir -p ./dk_util/new1
mkdir -p ./dk_util/new2
touch ./dk_util/new1/file{0..9}

while true ; 
do 
  for i in file0 file1 file2 file3 file4 file5 file6 file7 file8 file9
  do 
    echo _____ $i ; 
    date -u; 
    ls -l ./dk_util/new1/ ./dk_util/new2/
    /usr/bin/mv -f ./dk_util/new1/$i ./dk_util/new2/
  done 

  for i in file0 file1 file2 file3 file4 file5 file6 file7 file8 file9
  do 
    date -u;
    echo _____ $i ; 
    ls -l ./dk_util/new1/ ./dk_util/new2/
    /usr/bin/mv -f ./dk_util/new2/$i ./dk_util/new1/
  done 
done
