#!/usr/bin/python3.4
from lru_cache import *
import time
import socket
import sys
import threading
import os
import queue
import random
#config module
import readconfig
import errno
import math


class SaitamaNFSClient():

	def __init__(self, blockSize, blocks, cacheFreshnessT):

		self.block_size = blockSize
		self.blocks=blocks
		self.cacheFreshnessT = cacheFreshnessT
		self.cache = LRUCache(length=blocks, delta=cacheFreshnessT)
		self.cache_table = {} # for each fd which block we have in cache
		self.opened_files = {}
		self.address = None
		self.t1 = None
		self.t2 = None
		self.ToDo = queue.Queue()
		self.Rdy = queue.Queue()
		self.requests = {}
		self.maxRequests = 100
		self.upperBounds = 30720 #actual = 64K but just to be sure
		self.overhead = 100
		self.maxRequestSize = 1024

	def SaitamaNFSsetSrv(self, ipaddr, port):

		self.address = (ipaddr, port)
		if self.t1 == None and self.t2 == None:
			client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			# client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			# client_socket.bind(('localhost',port))
			self.t2 = threading.Thread(target= self.sender, args=(client_socket,))
			self.t2.start()
			self.t1 = threading.Thread(target= self.listener, args=(client_socket,))
			self.t1.start()
		else:
			print("Already know the server")
	
	def listener(self,socket):
		print("Waiting for data")
		data = None

		while True:
			print("Waiting for data")
			data, address = socket.recvfrom(self.block_size+self.overhead)
			data = data.decode()
			print("_____________________________________")
			print(data)
			print("_____________________________________")
			reqId ,data = data.split(":",1)

			if reqId !="terminal":
				data = data.split(":", 1)
				if len (data) > 1:
					res, data = data
				else:
					res=data[0]
				try:
					print(self.requests)
					fscall = self.requests[reqId][0]
				except:
					print("message format error")
					continue
				
			else:
				fscall = "terminal"
			# if reqId == self.block:
				#put message to blocking queue in adition
			print(fscall)
			if fscall =="open":
				del self.requests[reqId]
				if "1" == res:
					self.Rdy.put(data)
				else:
					self.Rdy.put(data)
			elif fscall =="read":
				if res =="2":
					if self.requests[reqId][1] ==True:
						self.Rdy.put((res,"",str(data)))
				elif res == "1":
					#put it into cache
					timestamps, data = data.split(":", 1)
					if self.requests[reqId][1] == True: #also unblock
						print("unblock app")
						self.Rdy.put((res,timestamps, data)) #send missing blocks
					# entries = math.ceil(len(data)/self.block_size)

					if self.requests[reqId][3] == "-1": # we sent request without having any timestamp 
						timestamps = timestamps.split(";")
						print(timestamps)
						fd = self.requests[reqId][2]
						# pos = self.requests[reqId][5]
						
						blockNum=int(self.requests[reqId][5])/self.block_size #starting block
						# print(blockNum)
						blockNum=int(blockNum)

						# print(data)
						# print(len(data))
						it=0
						while True:
							if not data:
								break
							length = (len(data) % self.block_size )+1
							# print("length "+str(length))
							# print("block number "+ str(blockNum))
							tm = timestamps[it].split(",")[1]
							self.cache.insertItem(LRUCacheItem(fd+":"+str(blockNum), data[:length], tm))
							try:
								self.cache_table[fd]=self.cache_table[fd]+blockNum
							except:
								self.cache_table[fd]=[blockNum]
							blockNum=blockNum+1
							it=it+1
							data=data[length:]
					else:
						timestamps=timestamps.split(";")
						entries = len(timestamps)
						print("entries:" + str(entries))

						fd = self.requests[reqId][2]

						if len(timestamps)==1 and timestamps[0]=="":
							print("all good")
						else:
							for it in timestamps:
								blockNum, timestamp = it.split(",")
								blockNum=int(blockNum)
								if not data:
									break
								length = self.block_size%len(data)
								try:
									index = self.cache_table[fd].index(blockNum)
									self.cache.removeItem(fd+":"+str(blockNum))
								except:
									try:
										self.cache_table[fd]=self.cache_table[fd]+blockNum
									except:
										self.cache_table[fd]=[blockNum]
									#we dont have it in cache
								self.cache.insertItem(LRUCacheItem(fd+":"+str(blockNum), data[:length], timestamp))
								data=data[length:]
				elif res == "0":
					# Error , mAYBE we want o resend request for it or just let client handle it
					if self.requests[reqId][1] == True:
						self.Rdy.put((res,"",data))
				del self.requests[reqId]
			elif fscall =="write":
				if res == "0" and self.requests[reqId][1] == True:
					self.Rdy.put((res, data))
				else:
					timestamps, data = data.split(":", 1)
					fd = self.requests[reqId][2]

					if self.requests[reqId][1] == True:
						self.Rdy.put((res, data))
					if len(timestamps) == 2 and timestamps=="-1": #we know all timestamps are 1 at server side
						blockNum = self.requests[reqId][5]/self.block_size
						
						while True:
							if not data:
								break
							pos = self.block_size%len(data)
							self.cache.insertItem(LRUCacheItem(fd+":"+str(blockNum), data[:pos], "1"))
							blockNum = blockNum + 1
							data=data[pos:]
					else:
						timestamps=timestamps.split(";")
						buff = self.requests[reqId][3]
						for it in timestamps:
							blockNum, timestamp = it.split(",")
							length = self.block_size%len(data)
							blockNum=int(blockNum)
							try:
								index = self.cache_table[fd].index(blockNum)
								self.cache.removeItem(fd+":"+str(blockNum))
							except:
								self.cache_table[fd]=self.cache_table[fd] + blockNum
								#we dont have it in cache
							self.cache.insertItem(LRUCacheItem(fd+":"+str(blockNum), buff[:length], timestamp))
							buff=buff[length:]
							# self.requests[reqId][3] = self.requests[reqId][3][length:]
				del self.requests[reqId]
			elif fscall =="close":
				if self.requests[reqId][1] == True:
					self.Rdy.put((res, data))
				del self.requests[reqId]

			elif fscall == "terminal":
				self.Rdy.put(data)
				
	def sender(self,socket):
		while True:
			print("Give me something to send")
			data = self.ToDo.get()
			print("SEND:"+str(data)+" to " + str(self.address))
			print(socket.sendto(data.encode(), self.address))


	def SaitamaNFSopen(self, fname, cacheFreshnessT):

		if fname[0] =="/" :
			fname = fname[1:]
		reqId = random.randint(0, self.maxRequests) # we can have at most maxRequests 'running'
		while reqId in self.requests.keys():
			reqId = random.randint(0, self.maxRequests)
		reqId=str(reqId)
		message = ":".join(["open",reqId, fname])
		self.requests[reqId]=("open",fname) # blocking task
		# print(self.requests)
		self.ToDo.put(message)
		
		fd = self.Rdy.get()
		if fd =="0":
			print ("something went wrong")
		else:
			print("Open succes.also prefetch 1st block")
			self.opened_files[fd] = 0 #for lseek
			self.SaitamaNFSread(fd,self.block_size,block=False) # prefetch some data,this is a non blocking read
		return fd

	def SaitamaNFSterminal(self, command):
		#file cannot be found or created
		message = ":".join(["terminal", command])
		while True:
			try:
				self.ToDo.put(message)
				res = self.Rdy.get(block=True, timeout=1)
				break
			except:
				print("Timeout:try sending request again")
		# print(self.Rdy.get())
		return res


	def SaitamaNFSread(self, fd, length, pos=0, block=True):
		#elegxos an exoume to fd stin cache -praktika klp
		#send desired position also
		fd = str(fd)
		pos = self.opened_files[fd] + pos # real position
		numblock = math.floor(pos/self.block_size)*self.block_size #where to start
		numblocks = int(math.ceil((length / self.block_size))) #how far to go
		data = []
		reqId = random.randint(0, self.maxRequests) # we can have at most maxRequests 'running'
		while reqId in self.requests.keys():
			reqId = random.randint(0, self.maxRequests)
		reqId=str(reqId)
		if fd in self.cache_table.keys():
			miss = []
			
			timestampV = ""
			for i in range(numblock, numblock + numblocks):
				cache_item = self.cache.getItem(str(fd)+":"+str(i))
				if cache_item == None: # we miss this certain block
					miss.append(i)
					timestampV=timestampV+str(-1)+";"
					# try:
					# 	self.cache_table[fd].pop(self.cache_table[fd].index(i))
					# except:
					# 	print()
					if i in self.cache_table[fd]: #delete praktika
						self.cache_table[fd] = [x for x in self.cache_table[fd] if x!=i]
						#or
						# self.cache_table[fd].pop(self.cache_table[fd].index(i))
				else:
					# print(cache_item.timestamp)
					timestampV = timestampV+str(cache_item.timestamp)+";"
					data.append((i, cache_item))
			timestampV = timestampV[:-1]
			if not data:
				timestampV=str(-1) #-1 means we know nothing about the staff we are asking 
			print("TimestampV: " + timestampV)

			message = ":".join(["read", reqId, str(fd), str(timestampV), str(numblocks* self.block_size), str(pos)])
			self.requests[reqId]=("read", block, str(fd), str(timestampV), str(numblocks* self.block_size), str(pos))
			self.ToDo.put(message)

		else:
			tm = -1
			length = math.ceil(length/self.block_size)
			# pos = self.block_size * numblocks
			message = ":".join(("read", reqId, str(fd), str(tm), str(numblocks * self.block_size), str(pos)))
			self.requests[reqId]=("read", block, str(fd), str(tm), str(numblocks * self.block_size), str(pos)) #Blocking read request
			self.ToDo.put(message)

		if block == True:
			res, timestampV, newData = self.Rdy.get()
			if res=="0":
				print("error writing")
				print(newData)
				return "-1"
			elif res=="2":
				print("EOF")
				return "0"
			
			i=0
			y=0
			answer=""
			if not data:
				answer = newData
			else:
				if not newData:
					for i in data:
						answer=answer+i[1].item
				else:
					
					timestampV=timestampV.split(";")
					# print(timestampV)
					while y < len(timestampV):
						blockNum, tm = timestampV[y].split(",")
						if i<len(data):
							if int(blockNum) < data[i][0]:
								index=self.block_size%len(newData)
								answer=answer+newData[:index]
								print("0."+answer)
								newData=newData[index:]
								print(newData)
								y=y+1
							elif int(blockNum) == data[i][0]:
								index=self.block_size%len(newData)
								answer=answer+newData[:index]
								print("1."+answer)
								newData=newData[index:]
								print(newData)
								i=i+1
								y=y+1
							else:
								answer=answer+data[i][1].item
								print("2."+answer)
								i=i+1
						else:
							index=self.block_size%len(newData)
							if index==0:
								index=1
							# print(index)
							answer=answer+newData[:index]
							print("3."+answer)
							newData=newData[index:]
							print(newData)
							y=y+1
						# elif data[i][0] == blockNum:
						# 	index=self.block_size%len(newData)
						# 	answer.append(newData[:index])
						# 	newData=newDatap[index:]
			if length<len(answer):
				width=length%len(answer)
			else: 
				width=length
			print(width)
			answer=answer[:width] #answer has Blocks of data 
			print(answer)
			self.opened_files[fd] = self.opened_files[fd] + len(answer)
			return answer

	def SaitamaNFSwrite(self, fd, buff, length, block=True):
		fd = str(fd)
		if str(fd) not in self.opened_files.keys():
			print("Stupid check fd")
			return
		position = self.opened_files[fd]
		if len(buff) < length:
			length = len(buff)

		reqId = random.randint(0, self.maxRequests)
		while reqId in self.requests.keys():
			reqId = random.randint(0, self.maxRequests)
		reqId=str(reqId)
		message = ":".join(("write", str(reqId), str(fd), str(buff[:length]), str(position)))
		self.requests[reqId]=("write", block, str(fd), str(buff[:length]), str(position))
		self.ToDo.put(message)
		if block:
			res, data = self.Rdy.get()
			if res=="1":
				if int(data)>0:
					self.opened_files[fd] = self.opened_files[fd] + int(data)
				return data
			else:
				# print("catch error")
				print(data) #print error
				return -1



	def SaitamaNFSseek(self, fd, offset):
		if str(fd) in self.opened_files.keys():
			self.opened_files[fd] = self.opened_files[fd] + offset
		else:
			return

	def SaitamaNFSclose(self, fd, block=False, cleanCache=True):
		#file cannot be found or created
		fd = str(fd)
		if fd not in self.opened_files.keys():
			return
		reqId = random.randint(0, self.maxRequests) # we can have at most maxRequests 'running' otherwise it with stuck forever here
		while reqId in self.requests.keys():
			reqId = random.randint(0, self.maxRequests)
		message = ":".join(["close",str(reqId), fd])
		self.requests[reqId]=("close", block, str(fd))
		
		self.ToDo.put(message)
		
		if block == True:
			res = self.Rdy.get()
			if res == "0":
				print ("something went wrong")  # we dont care though
			else:
				print("Success")
		if cleanCache:
			print("Cleaning")
			for i in self.cache_table[fd]:
				self.cache.removeItem(fd+":"+i)
			del self.cache_table[fd]
			del self.opened_files[fd]
			print("\r\n\nCleaning  done")

	def cacheItems(self):
		print("printing cache items")
		self.cache.printItems()
if __name__ == "__main__":

	#Get values from config file
	conf = readconfig.ReadConfig()
	conf.readConfiguration("config.ini")

	ip = conf.getValue("ip")
	port = int(conf.getValue("port"))
	block_size = int(conf.getValue("block_size"))
	blocks = int(conf.getValue("blocks"))
	cacheFreshnessT = int(conf.getValue("cacheFreshnessT"))
	print("Remote address: %s:%d" % (ip,port))
	print("Cache details:\n\tBlocks: "+str(blocks)+"\n\tBlock size: "+str(block_size/1024)+"KB\n\tCache freshness: "+str(cacheFreshnessT)+"s\n\tTotal size: "+str(blocks*block_size/1024)+"KB")
	nfs = SaitamaNFSClient(block_size, blocks, cacheFreshnessT)
	nfs.SaitamaNFSsetSrv(ip, port)

	

	while 1:
		choice = input("\n0.SaitamaNFS_terminal\n1.SaitamaNFS_open\n2.SaitamaNFS_read\n3.SaitamaNFS_write\n4.SaitamaNFS_seek\n5.SaitamaNFS_close\n6.SaitamaNFS_printCache\nEnter your choice: ")
		if choice =="":
			continue
		else:
			choice=int(choice)
		if choice == 0:
			command = input("Enter Command to execute at remote server: ")
			if command=="":
				continue
			nfs.SaitamaNFSterminal(command)

		elif choice == 1:
			fname = input("Enter fname: ")
			if fname=="":
				continue
			cacheFreshnessT = 0 #int(input("Enter cacheFreshnessT: "))
			fd = nfs.SaitamaNFSopen(fname, cacheFreshnessT)
			# if fd != -1:
			# 	print ('Filename: %s with fd: %d\n'%(fname,fd))
			
		elif choice == 2:
			# fd = int(input("Enter fd: "))
			n = int(input("Enter how many bytes you want to read: "))
			buf  = nfs.SaitamaNFSread(fd, n)
			print ("Read ", len(buf))
			print (buf)
			
		elif choice == 3:
			fd = int(input("Enter fd: "))
			n = int(input("Enter how many bytes you want to write: "))
			buf2 = input("Enter message to be written: ")
			nbytes = nfs.SaitamaNFSwrite(fd, buf2, n)
			print ("Wrote ", nbytes)
			
		elif choice == 4:
			fd = int(input("Enter fd: "))
			pos = int(input("Enter pos: "))
			nfs.SaitamaNFSseek(str(fd), pos)
			
		elif choice == 5:
			fd = int(input("Enter fd: "))
			nfs.SaitamaNFSclose(fd)
		elif choice == 6:
			nfs.cacheItems()
		else:
			break
