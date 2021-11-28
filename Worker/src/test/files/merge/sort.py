#!/usr/bin/python

import sys

raw = ""
with open(sys.argv[1], "r") as f:
    raw = f.read()

raw = raw.split("\r\n")[:-1]
raw.sort(key=lambda x: x[0:10])

with open(sys.argv[1] + "_sorted", "w") as f:
    f.write("\r\n".join(raw) + "\r\n")
