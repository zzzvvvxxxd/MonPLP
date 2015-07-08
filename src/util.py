#! /usr/bin/env python2.7
# coding=utf-8
__author__ = 'bulu_dog'
from dispatch import dispatch
def hashable(x):
	try:
		hash(x)
		return True
	except TypeError:
		return False

def transitive_get(key, d):
	while hashable(key) and key in d:
		key = d[key]
	return key


def diff_dict(dict1, dict2):
	for key, value in dict1.items():
		if dict2.has_key(key) and dict2[key] != value:
			return False
	return True