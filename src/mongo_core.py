#! /usr/bin/env python2.7
# coding=utf-8
__author__ = 'bulu_dog'

# import area
import datetime
import re
from pymongo import MongoClient
from pymongo import ReturnDocument
from dispatch import dispatch

'''
Mongo类
版本：2015/06/22
建立连接，主要用于获取指定数据库内特定的collection
'''


class Mongo(object):
	def __init__(self, name, host="localhost", port=27017):
		self.db = None
		self.name = name
		self.host = host
		self.port = port
		self.client = None
		self.initConnection(host, port)
		self.initDataBase()

	# get connection with MongoClient
	def initConnection(self, host="localhost", port=27017):
		self.client = MongoClient(host, port)
		# return self.client

	# get database with db name
	def initDataBase(self):
		self.db = self.client[self.name]

	# get collection by name
	def getCollection(self, cname):
		return self.db[cname]

	def close(self):
		self.client.close()


'''
term collection 代理类
版本：2015/06/22
term collection 建立索引情况：
(None)
'''


class TermCollection(object):
	def __init__(self, mongo = Mongo("logic")):
		# init function
		self.prefix = "T"
		self.name = "term"
		self.idTable = "termIdTable"
		self.mongo = mongo
		self.num = -1
		self.collection = self.mongo.getCollection(self.name)  # 获取term 和 idTable 集合
		self.delCollection = self.mongo.getCollection(self.idTable)
		self.counter = self.collection.find_one({"term": "__counter"})  # 验证term counter
		if (self.counter == None):
			# term counter 不存在则建立
			#print '------------------init [term] collection done------------------'
			self.collection.insert_one({"term": "__counter",
			                            "num": 0,
			                            "desc": "counter to make term collection'id inc automatically"})
		else:
			self.num = self.counter['num']
			# self.num = self.__parse_id(id_str)
			#print '-----------Load [term] collection done, all %d documents-----------' % self.num  # 成功信息，显示当前term数

	# 解析形如“T111”的term id，返回id number
	def __parse_id(self, id):
		pattern = self.prefix + '\d*'
		if re.match(pattern, id):
			return int(re.split(self.prefix, id)[1])
		return -1

	def __insert_one(self, document):
		term_name = document['term']
		_term_doc = self.collection.find_one({'term': term_name})
		if _term_doc != None:
			return _term_doc['id']
		# 检查id pool中是否有id，若有且小于当前num值，则取出作为id
		re = self.delCollection.find_one_and_delete({'id': {'$exists' : True}})
		if re:
			_new_id = re['id']
			new_id = self.prefix + str(_new_id)
			document["id"] = new_id
		else:
			# inc 1 to the num counter
			self.counter = self.collection.find_one_and_update({"term": "__counter"},
			                                                   {'$inc': {'num': 1}},
			                                                   return_document=ReturnDocument.AFTER)
			self.num = self.counter["num"]
			# set the term id
			new_id = self.prefix + str(self.num - 1)
			document["id"] = new_id
		# insert the term document
		self.collection.insert_one(document)
		return new_id

	# 删除一条记录
	def __delete_one(self, filter):
		try:
			del_doc = self.collection.find_one_and_delete(filter)
			_id = del_doc['id']
			id = self.__parse_id(_id)
			self.delCollection.save({'id': id})
			print "delete 1 document which contain {id:%d} successfully" % id
		except:
			print "[Error]delete error. Sure the doc in the term collection?"

	# 修正数据库，目前并不需要
	# 检查Term表是否存在异常，如果有异常，则修复
	def repair(self):
		# 检查计数是否正确
		self.num = self.collection.find_one({"term": "__counter"})['num']
		del_num = self.delCollection.count()
		term_num = self.collection.count() - 1
		# term表中counter的num字段应该 = del掉的id数量 + 现有term数量
		if self.num != del_num + term_num:
			print "[Error] num wrong"
			# <备份现有数据，导入term_wrong表>
			self._reduce()  # 回退数据库，出现重大逻辑错误，后期这里添加
		else:  # 若一致
			print "The term collection is all right"

	# 关闭数据库连接，断开term集合的操作
	def close(self):
		self.mongo.client.close()

	# 直接insert term，封装方法
	def insert_term(self, term):
		return self.__insert_one({'term': str(term)})

	# 直接delete term，封装方法
	def delete_term(self, term):
		self.__delete_one({'term': term})

	# find
	def find(self, filter=None):
		return self.collection.find(filter)

	# reduce
	# DEPRECATED - use [repair] to repair the term collection
	def _reduce(self):
		# 回退数据库到无数据版本,除非调试，否则不推荐使用
		# 删除term集合,重置counter
		self.collection.drop()
		self.collection.insert_one({"term": "__counter",
		                            "num": 0,
		                            "desc": "counter to make term collection'id inc automatically"})
		# 删除id表内容
		self.delCollection.drop()
		self.mongo.db.create_collection(self.idTable)

	def get_term_id(self, term):
		if isinstance(term, basestring) or isinstance(term, unicode):
			doc = self.collection.find_one({"term":term})
			if doc:
				return str(doc['id'])
			else:
				return False
		else:
			return False
			print "[ERROR]:term need to be string"

	def get_term_name(self, tid):
		if isinstance(tid, basestring) and 'T' in tid:
			doc = self.collection.find_one({'id':tid})
			if doc:
				return str(doc['term'])
			else:
				return False
		else:
			return False

	#查到single document
	def find_one(self, filter):
		return self.collection.find_one(filter)
	# Class TermCollection Done!


class RelationCollection(object):
	def __init__(self, mongo = Mongo("logic")):
		self.prefix = 'R'
		self.name = 'relation'
		self.idTable = 'relationIdTable'
		self.mongo = mongo
		self.collection = self.mongo.getCollection(self.name)
		self.delCollection = self.mongo.getCollection(self.idTable)
		self.counter = self.collection.find_one({"relation": "__counter"})  # 验证term counter
		if (self.counter == None):
			# term counter 不存在则建立
			#print '------------------init [relation] collection done------------------'
			self.collection.insert_one({"relation": "__counter",
			                            "num": 0,
			                            "desc": "counter to make relation collection'id inc automatically"})
		else:
			self.num = self.counter['num']
			# self.num = self.__parse_id(id_str)
			#print '---------Load [relation] collection done, all %d documents---------' % self.num  # 成功信息，显示当前term数

	# 解析形如“R111”的term id，返回id number
	def __parse_id(self, id):
		pattern = self.prefix + "\d*"
		if re.match(pattern, id):
			return int(re.split(self.prefix, id)[1])
		return -1

	def __insert_one(self, document):
		# 检查id pool中是否有id，若有且小于当前num值，则取出作为id
		rel_name = document['relation']
		_relation_doc = self.collection.find_one({'relation': rel_name})
		if _relation_doc != None:
			return _relation_doc['id']
		re = self.delCollection.find_one_and_delete({'id': {'$exists' : True}})
		if re:
			_new_id = re['id']
			new_id = self.prefix + str(_new_id)
			document["id"] = new_id
		else:
			# inc 1 to the num counter
			self.counter = self.collection.find_one_and_update({"relation": "__counter"},
			                                                   {'$inc': {'num': 1}},
			                                                   return_document=ReturnDocument.AFTER)
			self.num = self.counter["num"]
			# set the term id
			new_id = self.prefix + str(self.num - 1)
			document["id"] = new_id
		# insert the term document
		self.collection.insert_one(document)
		return new_id

	def insert_relation(self, rel):
		return self.__insert_one({'relation': str(rel)})

	# 删除一条记录
	def __delete_one(self, filter):
		try:
			del_doc = self.collection.find_one_and_delete(filter)
			_id = del_doc['id']
			id = self.__parse_id(_id)
			self.delCollection.save({'id': id})
			print "delete 1 document which contain {id:%d} successfully" % id
		except:
			print "[Error]delete error. Sure the doc in the term collection?"

	def delete_relation(self, relation):
		self.__delete_one({'relation': relation})

	# find()
	def find(self, filter=None):
		return self.collection.find(filter)

	def count(self):
		return self.collection.count()

	def _reduce(self):
		# 回退数据库到无数据版本,除非调试，否则不推荐使用
		# 删除term集合,重置counter
		self.collection.drop()
		self.collection.insert_one({"relation": "__counter",
		                            "num": 0,
		                            "desc": "counter to make relation collection'id inc automatically"})
		# 删除id表内容
		self.delCollection.drop()
		self.mongo.db.create_collection(self.idTable)

	def get_relation_id(self, relation):
		relation = str(relation)
		if isinstance(relation, basestring):
			doc = self.collection.find_one({"relation":relation})
			if doc:
				return str(doc['id'])
			else:
				return False
		else:
			print "[ERROR]:'relation' need to be string"
			return False

	def get_relation_name(self, tid):
		if isinstance(tid, basestring) and 'R' in tid:
			doc = self.collection.find_one({'id':tid})
			if doc:
				return str(doc['relation'])
			else:
				return False
		else:
			return False

	def generator(self):
		cursor =  self.collection.find()
		for doc in cursor:
			yield doc
	# end of RelationCollection

class RelationPool(object):
	def __init__(self):
		self.mongo = Mongo('logic')
		self.name = 'RelPool'
		self.collection = self.mongo.getCollection(self.name)

	def __insert_trigram(self, tid1, rid, tid2):
		self.collection.update_one({"relation":rid},{"$addToSet":{tid2 : tid1}}, True)	#upsert
		#self.collection.update_one({"term": rid},{"$addToSet":{tid1 : tid2}}, True)

	@dispatch(basestring, basestring, basestring)
	def insert(self, tid1, rid, tid2):
		self.__insert_trigram(str(tid1), str(rid), str(tid2))

	def _reduce(self):
		self.collection.drop()

	#返回相关实体的list
	def find_related_term(self, relation):
		doc = self.collection.find_one({'relation':relation}, projection={'_id':False, 'relation':False})
		if doc:
			for key in doc:
				for it in doc[key]:
					yield [it, key]
			else:
				return
		else:
			return

if __name__ == "__main__":
	r = RelationPool()
	for i in r.find_related_term('R0'):
		print i