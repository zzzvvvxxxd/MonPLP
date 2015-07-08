#! /usr/bin/env python2.7
# coding=utf-8
__author__ = 'bulu_dog'

from contextlib import contextmanager
from util import hashable
from dispatch import dispatch

_global_logic_variables = set()
_glv = _global_logic_variables

class Var(object):
	_id = 1
	def __new__(cls, *token):
		if len(token) == 0:
			token = "_%s" % Var._id     #参数为空,表示是匿名变量,赋值为_id,id为该匿名变量的后台id,也反应了系统中当前匿名变量的个数
			Var._id += 1
		elif len(token) == 1:
			token = token[0]

		obj = object.__new__(cls)
		obj.token = token
		return obj

	def __init__(self, *token):
		self.value = None

	def __str__(self):
		return "~" + str(self.token)
	__repr__ = __str__

	def __eq__(self, other):
		return type(self) == type(other) and self.token == other.token

	def __hash__(self):
		return hash((type(self), self.token))

	def set(self, value):
		if self.value == None:
			self.value = value
			return True
		else:
			if self.value != value:
				return True
			else:
				return False

#返回的值，在概念上就是变量1，而非数字1（当然其实是Var类，其token为1）
#打印出来就是 ~1 ，这样便于区别变量和常量
var = lambda *args: Var(*args)

#vars()
#返回的是n个匿名变量组成的list
#例如：vars(3)
#返回的值就是[~_1, ~_2, ~_3]
vars = lambda n: [var() for i in range(n)]


@dispatch(Var)
def isvar(v):
	return True

@dispatch(object)
def isvar(o):
	return isinstance(o, Var)

if __name__ == "__main__":
	x = var('x')
	print isvar('var')
