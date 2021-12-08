#!/usr/bin/python

import os
import sys
from os import listdir
from os.path import isfile, isdir, join

def checkOneDirectory(dirPath):
  fileList = [f for f in listdir(dirPath) if isfile(join(dirPath, f))]
  fileList.sort(key=lambda x: int(x[len("partition."):]))
  print(fileList)

  checkList = []
  os.chdir(dirPath)
  minKey = "\x00" * 10
  for f in fileList:
      raw = ""
      with open(f, "r") as stream:
          raw = stream.read()
      l = raw.split("\r\n")[:-1]
      print(f, all(l[i][0:10] <= l[i + 1][0:10] for i in range(len(l) - 1)))
      print(l[0][0:10], l[-1][0:10])
      checkList.append((l[0][0:10], l[-1][0:10]))

  result = all(checkList[i][1] <= checkList[i + 1][0] for i in range(len(checkList) - 1))
  print("Check", dirPath, ":", result)
  return (checkList[0][0], checkList[-1][1], result)

print(checkOneDirectory(sys.argv[1]))
