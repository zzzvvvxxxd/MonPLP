#! /usr/bin/env python2.7
# coding=utf-8
__author__ = 'bulu_dog'

f = open("1.txt", 'r+')
line = f.readline()
print line
"""
offset: 文件的读/写指针位置.

whence: 这是可选的，默认为0，这意味着绝对的文件定位，其他值是1，这意味着当前的位置和2手段寻求相对寻求相对文件的结束.
"""
f.seek(-1, 1)
f.write('hello')
print f.readline()
f.close()
