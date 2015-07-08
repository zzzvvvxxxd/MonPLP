#! /usr/bin/env python2.7
# coding=utf-8
__author__ = 'bulu_dog'

from util import transitive_get
from collections import Iterator
from dispatch import dispatch
from variable import isvar, Var, var
from toolz import assoc

seq = tuple, list, Iterator
@dispatch(seq, seq, dict)
def _unify(u, v, s):
	# assert isinstance(u, tuple) and isinstance(v, tuple)
	if len(u) != len(v):
		return False
	for uu, vv in zip(u, v):  # avoiding recursion
		s = unify(uu, vv, s)
		if s is False:
			return False
	return s

@dispatch(object, object, dict)
def _unify(u, v, s):
	return False  # catch all

def unify(u, v, s):
	u = transitive_get(u, s)
	v = transitive_get(v, s)
	if u == v:
		return s
	if isvar(u):
		return assoc(s, u, v)       #返回新的匹配组
	if isvar(v):
		return assoc(s, v, u)
	return _unify(u, v, s)

if __name__ == "__main__":
	x = var('x')
	y = var('y')
	d = unify([x, 'R0', 'T1'], ['T0', 'R0', 'T1'], {})
	print d
