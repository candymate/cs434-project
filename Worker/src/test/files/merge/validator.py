#!/usr/bin/python

from os import listdir
from os.path import isfile, join
import os
import sys

fileList = [f for f in listdir(sys.argv[1]) if isfile(join(sys.argv[1], f))]
fileList.sort()
print(fileList)

checkList = []
os.chdir(sys.argv[1])
minKey = "\x00"*10
for f in fileList:
  raw = ""
  with open(f, "r") as stream:
    raw = stream.read()
  l = raw.split("\r\n")[:-1]
  print(f, all(l[i][0:10] <= l[i+1][0:10] for i in range(len(l)-1)))
  print(l[0][0:10], l[-1][0:10])
  checkList.append((l[0][0:10], l[-1][0:10]))

print(all(checkList[i][1] < checkList[i+1][0] for i in range(len(checkList) - 1)))
