#!/bin/sh

help() {
  echo ""
  echo "[*] Usage: $0 -d destination-directory -p partition-number"
  exit 1
}

while getopts "d:p:h" opt
do
  case "$opt" in
    d ) dest="$OPTARG" ;;
    p ) partnum="$OPTARG" ;;
    h ) help ;;
    ? ) help ;;
  esac
done

if [ -z "$dest" ]
then
  echo "[!] destination directory empty"
  help
fi

if [ -z "$partnum" ]
then
  echo "[!] partition num not set"
  help
fi

curDir=$PWD
cd $dest
for i in $(seq 0 $(($partnum - 1)));
do
  idx_start=$(($i * 320000))
  gensort -a "-b${idx_start}" 320000 "partition${i}"
done

