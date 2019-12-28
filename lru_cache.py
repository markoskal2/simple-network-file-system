from datetime import datetime

class LRUCacheItem(object):
	"""Data structure of items stored in cache"""
	def __init__(self, key, item, timestamp):
		self.key = key
		self.item = item
		self.time = datetime.now() #time timestamp
		self.timestamp = timestamp # version timestamp


class LRUCache(object):
	"""A sample class that implements LRU algorithm"""

	def __init__(self, length, delta=None):
		self.length = length
		self.delta = delta
		self.hash = {}
		self.item_list = []
		self.size = 0

	def insertItem(self, item):
		"""Insert new items to cache"""

		if item.key in self.hash.keys():
			# Move the existing item to the head of item_list.
			item_index = self.item_list.index(item.key) # get past position
			self.item_list[:] = self.item_list[:item_index] + self.item_list[item_index+1:] # rebuild cache without "past position"
			self.item_list.insert(0, item.key) # insert item on head
		else:
			# Remove the last item if the length of cache exceeds the upper bound.
			if self.size > self.length:
				index = len(self.item_list)-1 # last item (lru)
				self.removeItem(self.item_list[index])

			# If this is a new item, just append it to
			# the front of item_list.
			self.size = self.size + len(item.item)
			self.hash[item.key] = item
			self.item_list.insert(0, item.key)

	def getItem(self, key):
		if key in self.hash.keys():
			item_index = self.item_list.index(key) # get past position
			print("index:    "+str(item_index))
			self.item_list = self.item_list[:item_index] + self.item_list[item_index+1:] # rebuild cache without "past position"
			self.item_list.insert(0, key) # insert "past position" on head
			print(self.hash[key])
			return self.hash[key]
		else:
			return None

	def removeItem(self, key):
		"""Remove those invalid items"""
	
		if key in self.hash.keys():
			del self.item_list[self.item_list.index(key)]
			del self.hash[key]

	def printItems(self):
		for k,v in self.hash.items():
			print(k)
			print(v)
		

	def validateItem(self):
		"""Check if the items are still valid."""

		def _outdated_items():
			now = datetime.now()
			for item in self.hash.values():
				time_delta = now - item.time
				if time_delta.seconds > self.delta:
					yield item
		map(lambda x: self.removeItem(x), _outdated_items())