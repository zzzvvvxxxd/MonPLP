#! /usr/bin/env python2.7
# coding=utf-8
import json

f = open("./data/output.json", "r")
line = f.readline().rstrip(",")
dict = json.loads(line)
print dict['labels']['en']
print dict.keys()
print dict['sitelinks']['enwiki']
print dict['descriptions']['en']['value']