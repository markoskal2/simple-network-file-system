#!/usr/bin/python3

import random
import socket
import queue
import sys
import threading
import os
from pathlib import Path
import random
import math

import readconfig

CREATE, FIND = (0, 1)
class FileHandling():
	# Flag means create it if not exists
	def openFile(path, flag=CREATE):
		flag=int(flag)
		done="1"
		file = None
		print(path)
		# os.system("cat "+ path)
		print()
		try:
			fd = os.open(path,os.O_RDWR | os.O_CREAT)
			done = "1" + str(fd)
		except (IOError,OSError) as e:
			done="0"+str(e)
		finally:
			try:
				os.close(fd)
			except:
				i=0

		return done


	def readFile(path,num,pos=0):
		num = int(num)
		pos = int(pos)
		# fd=None
		try:
			byte= "1"
			fd = os.open(path,os.O_RDWR)
			position = os.lseek(fd, pos, os.SEEK_SET)
			if position != pos:
				print("to perasa")
				os.close(fd)
				byte="1"
				return byte
			read = os.read(fd, num).decode()
			print("READ " +str(read))
			byte = byte + read
		except(OSError, IOError) as e:
			# print("HERE")
			print(e)
			byte = "0" + str(e)
		finally:
			os.close(fd)
		return byte

		

	def writeFile(path,array,pos=0):
		pos=int(pos)
		f=None
		try:
			byte= "1"
			fd=os.open(path,os.O_RDWR)
			position = os.lseek(fd, pos, os.SEEK_SET)
			if position != pos:
				os.close(fd)
				byte="0"
				return byte
			# array=array.decode()
			write = os.write(fd, array.encode())
			byte=byte+str(write)
		except(OSError, IOError) as e:
			print(e)
			byte = "0" + str(e)
		finally:
			os.close(fd)
		return byte

FSCALL, ID, FILE, OPTS, POS, FIVE = (0, 1, 2, 3, 4, 5)
class FileSystemServer():

	def __init__(self,port=8888,path="~/",blockSize=1024):
		self.port = port
		self.maxOpened = 1000
		self.receive=1024
		self.path = path
		self.q = queue.Queue()
		self.block_size = blockSize
		self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.server_socket.bind(('', port))
		self.files = {}
		self.pastRequests = {}
		self.fds = {}
		os.chdir(os.path.dirname(path))
		self.t1=[]
		self.t2=[]
	def startWorkers(self,num):
		for i in range(0,num):
			t1 = threading.Thread(target= self.listener)
			t1.start()
			self.t1.append(t1)
			t2 = threading.Thread(target= self.sender)
			t2.start()
			self.t2.append(t2)

	def isInDict(self,lista,value):
		for k, v in lista.items():
			if value in v:
				return k
		return None


	def listener(self):
		print("Waiting for data")
		data = None
		notifications=[]
		while True:
			# while(data= self.server_socket.recvfrom(self.bufferSize) !=)
			message, address = self.server_socket.recvfrom(self.receive)
			message= message.decode()
			print(message +" from")
			print(address)
			# iterate the requests
			fields = message.split(":", 5)
			# for i in range(0,len(fields)):
			# 	print(fields[i]+" " +str(i))
			if fields[FSCALL] == "open":
				fd = random.randint(0, self.maxOpened)
				flag = False
				for k, v in self.files.items():
					if fields[FILE] in v:
						fd = k
						flag=True
						break
				while (flag==False and fd in self.files.keys()): #generate random id for opened file
					fd = random.randint(0,self.maxOpened)
				print(fd)
				fd = str(fd)
				if flag==True:
					message = ":".join([fields[ID], "1", fd])
				else:
					res = FileHandling.openFile(fields[FILE])
					# print(res)

					if res[0] == "1":
						if fd not in self.fds.keys():
							self.fds.update({fd:address})
						else:
							self.fds[fd] = self.fds[fd] + address
						name = fields[FILE].split("/")
						print(name)
						self.files[fd]=str(os.path.abspath(path))+"/"+name[len(name)-1] #timestamp 0(means version of while)
						print(self.files[fd])
						message = ":".join([fields[ID], res[1:], fd])
						# message = fields[FSCALL]+":"+fields[FILE]+":"+res[:1]+":"+fd
					else:
						message = ":".join([fields[ID], res[:1], res[1:]])
						# message = fields[FSCALL]+":"+fields[FILE]+":"+res[:1]+":"+res[1:]
				
			elif fields[FSCALL] =="write":
				# if len(fields) == 3:
				# 	pos=0
				# else:
				pos=fields[POS]
				if fields[FILE] not in self.files.keys():
					message = ":".join([fields[ID], "0", "incorrect file descriptor"])
					# message = fields[FSCALL]+":"+fields[FILE]+":"+"0"+":"+"incorrect file descriptor"
				else:

					res = FileHandling.writeFile(self.files[fields[FILE]],fields[OPTS], pos)
					message = ":".join([fields[ID], res[:1]])
					# message = fields[FSCALL]+":"+fields[FILE]+":"+res[:1]+":"+res[1:]
					if res[0] =="1":
						print("Successful write")
						res = res[1:]
						wrote = len(res)
						entries = math.ceil(wrote/self.block_size)
						blockNum = int(int(pos)/self.block_size)
						entries = [ i for i in range(blockNum,blockNum + entries)]
						if fd not in self.pastRequests.keys():
							tm = []
							for it in entries:
								tm.append(1)
							self.pastRequests[fd] = (entries,tm)
							message = ":".join([message,str(-1)])
						else:
							message=message+":"
							for it in entries:
								gold = self.existsRequest(fd, it)
								if gold==None:
									self.pastRequests[fd] = (self.pastRequests[fd][0]+[it],self.pastRequests[fd][1]+[1])
									timestamp = ",".join([str(it), str(1)])
								else:
									self.pastRequests[fd][0][gold] =  self.pastRequests[fd][0][gold] + 1
									timestamp = ",".join([str(it), str(self.pastRequests[fd][0][gold])])
								# except:
								# 	# temp = ([address],0)
								# 	self.pastRequests[fd] = (self.pastRequests[fd][0]+[it],self.pastRequests[fd][1]+[1])
								# 	timestamp = ",".join([str(it), str(1)])
								# finally:
								message = message+timestamp+";"
							message=message[:-1]
						message=":".join([message,res])
					else:
						message = ":".join([message,res])
						print("Unsuccessful write")
			elif fields[FSCALL] =="read":
				# if len(fields) == 3:
				# 	pos=0
				# else:
				fd = fields[FILE]
				reqId = fields[ID]
				timestampV = fields[OPTS]
				length = fields[POS]
				pos = fields[FIVE]

				if fd not in self.files.keys():
					message = ":".join([reqId, "0", "incorrect file descriptor"])
				else:
					# print(self.files[fd])
					# print(length)
					# print(pos)
					
					entries = math.ceil(int(length)/self.block_size) # how many entries want to read
					blockNum = int(pos)/self.block_size # starting block?

					timestampV = timestampV.split(";")
					print(timestampV)
					if len(timestampV) == 1 and timestampV[0] == "-1":
						#you know nothing John
						res = FileHandling.readFile(self.files[fd], length, pos)
						# print(len(res)-1)
						# print("@@@")
						if len(res)==1 and res=="1":
							message = ":".join([reqId, "2"])
						elif res[0] == "1":
							message = ":".join([reqId, res[:1],""])

							for i in range(0,entries):
								blockNUMBER = int((i * self.block_size) +blockNum)
								# print(blockNUMBER)
								try:
									index = self.pastRequests[fd][0].index(blockNUMBER)
									timestamp=",".join([str(blockNUMBER),str(self.pastRequests[fd][0][index])])
								except:
									try:
										self.pastRequests[fd]=(self.pastRequests[fd][0] + [blockNUMBER] ,self.pastRequests[fd][1]+[0])
									except:
										self.pastRequests[fd] = ([blockNUMBER],[0])
									timestamp=",".join([str(blockNUMBER),str(0)])
								finally:
									# print(timestamp)
									message = message +timestamp+";"
							message=message[:-1]
							message = ":".join([message, res[1:]])
						else:
							print("error")
							message = ":".join([message, res[1:]])
					else:
						data=""
						i = blockNum
						newtimestampV = ""
						flag=False
						for it in timestampV:
							tm = it
							block= int(i)
							# print(block)
							print(self.pastRequests[fd])
							index = self.existsRequest(fd, block)
							if index==None:
								print("dont have previously")
								self.pastRequests[fd]=(self.pastRequests[fd][0]+[block], self.pastRequests[fd][1]+[0])
								# index = self.pastRequests[fd][0].index(block)
								index=len(self.pastRequests[fd][0])-1
							if int(tm) < self.pastRequests[fd][1][index]:
								# print(self.files[fd])
								# print(self.block_size)
								# print(str(i) +" * "+ str(self.block_size) +" "+str(int(i*self.block_size)))
								res = FileHandling.readFile(self.files[fd], self.block_size, int(i*self.block_size))
								# print(res)
								if res[0] == "1":
									print("Successful read")
									blockindex = i/self.block_size
									blockindex=int(blockindex)
									newE=",".join([str(blockindex), str(self.pastRequests[fd][1][index])])
									newtimestampV=newtimestampV +newE+";"
									data = data+res[1:]
									flag=True
								else:
									print(res[1:])
							else:
								flag=True
								print("YOU RIGHT TIMESTAMP FOR THIS BLOCK MATE")
								#client has valid data about this block
							i = i + 1

						if flag==True:
							newtimestampV=newtimestampV[:-1]
							message=":".join([reqId,"1", newtimestampV,data])
						else:
							message=":".join([reqId,"0", "Error: brains not found"])

				#send answer back
			elif fields[FSCALL] == "close":
				try:
					del self.fds[fields[FILE]]
					message = ":".join([fields[ID], "1", "success"])
				except KeyError as e:
					message = ":".join([fields[ID], "0", "incorrect file descriptor"])
					
			elif fields[FSCALL] =="terminal":
				#"cd "+ self.path +" && "+
				if "cd" == fields[1][:2]:
					try:
						message = ":".join([fields[FSCALL], "success"])
					
						if fields[1][:len(fields[1])] != "/":
							fields[1] = fields[1] + "/"
						print(fields[1][2:])
						os.chdir(os.path.dirname(fields[1][3:]))
					except (OSError, IOError,Exception) as e:
						message=":".join([fields[FSCALL], str(e)])
				else:
					message = ":".join([fields[FSCALL], os.popen(fields[1]).read()])
				#os.chdir(os.path.dirname(os.getcwd()))
			else:
				message="unknown command"
				print(message)
			print("answer to request")
			print(message)
			self.q.put((message, address))
	def existsRequest(self, fd,block):
		for itera in self.pastRequests[fd][0]:
			if itera==block:
				return itera
		return None
	def sender(self):

		print("Give me some data to send")

		while True:
			message, address = self.q.get()
			print("sending "+ message)
			if message == "exit" :
				break
			self.server_socket.sendto(message.encode(), address)
	def join(self):
		for i in range(0,len(self.t1)):
			self.t1[i].join()
			self.t2[i].join()
		
if __name__ == "__main__":

	conf = readconfig.ReadConfig()
	conf.readConfiguration("config.ini")
	port = int(conf.getValue("port"))
	path = conf.getValue("path")
	blockSize = int(conf.getValue("block_size"))
	workers = int(conf.getValue("workers"))
	print(path)
	print("Server running at port %d"  % port)

	s = FileSystemServer(port,path,blockSize)
	s.startWorkers(workers)
	s.join()
	# t1.join()
	# t2.join()

