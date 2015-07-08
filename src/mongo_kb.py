#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
__author__ = 'bulu_dog'
from dispatch import dispatch
from collections import Iterable
from mongo_core import Mongo, TermCollection, RelationCollection, RelationPool
import logging
from variable import Var, isvar, var
from unify import unify
from util import diff_dict

logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

def get_mode(t1, r, t2):
	mode = []
	mode.append(0 if isvar(t1) else 1)
	mode.append(0 if isvar(r) else 1)
	mode.append(0 if isvar(t2) else 1)
	return tuple(mode)


class KnowledgeBase(object):
	def __init__(self):
		self.name = "knowledge_base"
		self.mongo = Mongo("logic")
		self.term = TermCollection(self.mongo)
		self.relation = RelationCollection(self.mongo)
		self.pool = RelationPool()
		self.collection = self.mongo.getCollection(self.name)
		self.idPool = {}
	#subject-predicate-object

	def __insert_triple(self, triple):
		tid = self.term.insert_term(triple[0])
		rid = self.relation.insert_relation(triple[1])
		_tid = self.term.insert_term(triple[2])
		self.collection.update_one({"term": tid},{"$addToSet":{rid : _tid}}, True)
		self.pool.insert(tid, rid, _tid)

	@dispatch(dict)
	def insert_triple(self, doc):
		if not (doc.has_key('term')
		        and doc.has_key('attribute')
		        and isinstance(doc['term'], basestring)
		        and isinstance(doc['attribute'], dict)):
			print "[ERROR]: doc in wrong format"
			return
		#term
		_t = doc['term']
		_r = doc['attribute']
		tid = self.term.insert_term(_t)                 #insert term to 'term' collection
		for _relation, _term in _r.items():
			_rid = self.relation.insert_relation(_relation)
			#insert triple to knowlege_base
			_tid = self.term.insert_term(_term)
			self.collection.update_one({"term": tid},{"$addToSet":{_rid : _tid}}, True)
			self.pool.insert(tid, _rid, _tid)

	#(subject, predicate, object)
	@dispatch(list)
	def insert_triple(self, triple):
		_subject = triple[0]
		if isinstance(_subject, basestring):
			self.__insert_triple(triple)
		if isinstance(_subject, list):
			for item in triple:
				self.__insert_triple(item)

	def _reduce(self):
		self.term._reduce()
		self.pool._reduce()
		self.relation._reduce()
		self.collection.drop()

	def __find_one(self, filter_or_id=None, *args, **kwargs):
		return self.collection.find_one(filter_or_id, *args, **kwargs)

	#三元组匹配
	def find(self, t1 = None, r = None, t2 = None):
		mode = get_mode(t1, r, t2)
		if mode == (1,1,1) or mode == (0,0,0):
			yield False
		if mode == (1,1,0):
			doc = self.collection.find_one({'term':t1}, projection={'_id':False, 'term':False})
			if doc:
				if doc.has_key(r):
					for item in doc[r]:
						yield list([t1, r, item.encode('utf-8')])
				else:
					yield False
			else:
				yield False
		if mode == (1,0,0):
			doc = self.collection.find_one({'term':t1}, projection={'_id':False, 'term':False})
			if doc:
				for key, value in doc.items():
					for item in value:
						yield [t1, key.encode('utf-8'), item.encode('utf-8')]
			else:
				yield False
		if mode == (0,0,1):
			yield False
		if mode == (0,1,0):
			docs = self.pool.find_related_term(r)
			if not docs:
				yield False
			for item in docs:
				if not item:
					return
				yield [item[0], r, item[1]]
		if mode == (0,1,1):
			res = self.pool.find_related_term(r, t2)
			if res:
				for item in res:
					yield [item.encode('utf-8'), r, t2]
			else:
				yield False
		if mode == (1, 0, 1):
			s = []
			doc = self.collection.find_one({'term':t1}, projection={'_id':False, 'term':False})
			if doc:
				for key,value in doc.items():
					if t2 in value:
						yield [t1, key.encode('utf-8'), t2]
				yield False
			else:
				yield False

	@dispatch(list)
	def unification(self, triple):
		if len(triple) == 3:
			t1 = triple[0]
			r = triple[1]
			t2 = triple[2]
			t1 = t1 if isvar(t1) else self.term.get_term_id(t1)
			r  = r  if isvar(r)  else self.relation.get_relation_id(r)
			t2 = t2 if isvar(t2) else self.term.get_term_id(t2)
			generator = self.find(t1, r, t2)
			for item in generator:
				ans = unify(item, self.parse_triple_to_id(triple), {})
				if isinstance(ans, dict):
					for key in ans.keys():
						if 'T' in ans[key]:
							ans[key] = self.term.get_term_name(ans[key])
						else:
							ans[key] = self.relation.get_relation_name(ans[key])
					yield ans
				else:
					print triple, "unable to unify:", ans
					yield False
		else:
			yield False

	def parse_triple_to_id(self,triple):
		if (len(triple) == 3):
			r = triple[1] if isvar(triple[1]) else self.relation.get_relation_id(triple[1])
			t1 = triple[0] if isvar(triple[0]) else self.term.get_term_id(triple[0])
			t2 = triple[2] if isvar(triple[2]) else self.term.get_term_id(triple[2])
			return list((t1, r, t2))
		return False

	@dispatch(list, list, list)
	def resolution(self, relation, factor1, factor2):
		gen1 = self.unification(factor1)
		gen2 = self.unification(factor2)
		for sub1 in gen1:
			if not sub1:
				yield False
			for sub2 in gen2:
				if not sub2:
					return
				if diff_dict(sub1, sub2):
					ans = dict(sub1, **sub2)
					temp = []
					for i in range(len(relation)):
						if isvar(relation[i]):
							temp.append(ans[relation[i]])
						else:
							temp.append(relation[i])
					yield temp
			gen2 = self.unification(factor2)
	#end of knowledge_base class

if __name__ == "__main__":
	kb = KnowledgeBase()
	kb._reduce()
	kb.insert_triple(['zwq','is_friend','pd'])
	kb.insert_triple(['zwq','is_friend','ss'])
	kb.insert_triple(['pd' ,'is_friend','wb'])
	kb.insert_triple(['ss' ,'is_friend','zht'])
	x = var('x')
	y = var('y')
	for i in kb.resolution(['zwq','is_friend', y], ['zwq', 'is_friend', x], [x, 'is_friend', y]):
		print i
