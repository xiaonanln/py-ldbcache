# -*- coding: utf8 -*-


# ldbcache is lru cache using LevelDB as storage backend
# Data will be stored in lru cache and when there are too many items in memory, least recently used items will be moved
# to LevelDB storage.

from pylru import lrucache
import leveldb
import shutil
import os

_DEBUG_CORRECTNESS = False

class ldbcache(object):
	def __init__(self, name, ramsize, dbsize, marshal, unmarshal, cleardb=True):
		self.ramcache = lrucache(ramsize, self._onRemoveFromRam)
		self.dbcache = lrucache(dbsize, self._onRemoveFromDB)

		self.marshal = marshal
		self.unmarshal = unmarshal

		ldbdir = name + '.ldbcache'
		if cleardb:
			if os.path.exists(ldbdir):
				shutil.rmtree(ldbdir)

		self.ldb = leveldb.LevelDB(ldbdir)
		# if cleardb == False, restore all keys from DB
		if not cleardb:
			for key in self.ldb.RangeIter('', include_value=False):
				self.dbcache[key] = 1

	def _onRemoveFromRam(self, key, val):
		# if key-value is removed from RAM, put it to the DB
		# print 'MOVE FROM RAM TO DB: %s = %s' % (`key`, `val`)
		self.dbcache[key] = 1
		self.ldb.Put(key,  self.marshal(val))

	def _onRemoveFromDB(self, key, val):
		# print 'REMOVE FROM DB: %s = %s' % (`key`, `val`)
		self.ldb.Delete(key)

	def __len__(self):
		return len(self.ramcache) + len(self.dbcache)

	def clear(self):
		self.ramcache.clear()
		self.dbcache.clear()

	def __contains__(self, key):
		return key in self.ramcache or key in self.dbcache

	def __getitem__(self, key):
		# Look up the node
		try:
			return self.ramcache[key] # try to find the key in RAM cache
		except KeyError:
			# key  not found in ram cache, try to find in DB cache
			del self.dbcache[key]

			data = self.ldb.Get(key)
			val = self.unmarshal( data )

			# put key, val to ramcache, this might cause other key-value to be moved from RAM to DB
			self.ldb.Delete(key)
			self.ramcache[key] = val
			if _DEBUG_CORRECTNESS: self._checkCorrectness()
			return val

	def get(self, key, default=None):
		"""Get an item - return default (None) if not present"""
		try:
			return self[key]
		except KeyError:
			return default

	def __setitem__(self, key, value):
		# First, see if any value is stored under 'key' in the cache already.
		# If so we are going to replace that value with the new one.
		if not isinstance(key, (buffer, basestring)):
			raise TypeError('key must be string or buffer')

		if key in self.dbcache:
			# data in db, should remove it from DB and set key-val in RAM
			del self.dbcache[key]
			self.ldb.Delete(key)

		self.ramcache[key] = value
		if _DEBUG_CORRECTNESS: self._checkCorrectness()

	def __delitem__(self, key):
		# Lookup the node, then remove it from the hash table.
		try:
			del self.ramcache[key]
		except KeyError:
			del self.ramcache[key]
			self.ldb.Delete(key)

		if _DEBUG_CORRECTNESS: self._checkCorrectness()

	def __iter__(self):
		# Return an iterator that returns the keys in the cache in order from
		# the most recently to least recently used. Does not modify the cache
		# order.
		for key in self.ramcache:
			yield key

		for key in self.dbcache:
			yield key

	def items(self, ramonly=False):
		for item in self.ramcache.items():
			yield item

		if not ramonly:
			for key in self.dbcache:
				value = self.unmarshal(self.ldb.Get(key))
				yield (key, value)

	def keys(self, ramonly=False):
		for key in self.ramcache:
			yield key

		if not ramonly:
			for key in self.dbcache:
				yield key

	def values(self, ramonly=False):
		for value in self.ramcache.values():
			yield value

		if not ramonly:
			for key in self.dbcache:
				yield self.unmarshal(self.ldb.Get(key))

	def _checkCorrectness(self):
		for key in self.ramcache: # key should never in both ram and db
			assert key not in self.dbcache

		for key in self.dbcache: # key in dbcache must be saved in DB
			self.ldb.Get(key)

	def clear(self, ramonly=False):
		self.ramcache.clear()
		if not ramonly:
			for key in self.dbcache:
				self.ldb.Delete(key)
			self.dbcache.clear()

	def flush(self):
		"""save all ram cache to db cache"""
		# move all RAM items to DB
		for key, val in self.ramcache.items():
			self.dbcache[key] = 1
			self.ldb.Put( key, self.marshal(val) )

		self.ramcache.clear()

if __name__ == '__main__':
	c = ldbcache('test', 2, 2, str, int, cleardb=False)
	print 'items restored', list(c.items())

	c['1'] = 1
	c['2'] = 2
	c['3'] = 3
	c['4'] = 4
	c['5'] = 5
	print 'items', list(c.items())
	c['2'] = 2
	print 'items', list(c.items())
	print 'ram', list(c.items(ramonly=True))