#! /usr/bin/env python2.7
# coding=utf-8
__author__ = 'bulu_dog'


from multipledispatch import dispatch
from functools import partial

namespace = dict()

dispatch = partial(dispatch, namespace=namespace)

#Multiple Dispatch
#Doc： http://multiple-dispatch.readthedocs.org/en/latest/#
