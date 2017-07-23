#!/usr/bin/python
import json
import urllib2
import time
import datetime
import sys
from time import sleep

config = {"hostname":"127.0.0.1","port":"4985","sgDb":"sync_gateway","secure":False,"debug":True}

class WORK():

	hostname = '127.0.0.1'
	port = '4985'
	sgDb = 'sync_gateway'
	debug = False
	secure = "http"
	chkpt ='defaultChkPtNameChangeMe'

	def __init__(self,config):
		self.hostname = config["hostname"]
		self.port = config["port"]
		self.sgDb = config["sgDb"]
		self.debug = config["debug"]
		if config["chkPt"] != "":
			self.chkpt = config["chkPt"]
		if config["secure"] == True:
			self.secure = "https"
		else:
			self.secure = "http" 

	##--------Common Methods BEGIN---------##
	def sayHelloTest(self):
		print "hello"

	def httpGet(self,url='',retry=0):

		'''
		#url = urllib.urlencode(url)
		#values = { 'username': self.username,'password': self.password }
		#data = urllib.urlencode(values)
		#req = urllib2.Request(url, data)
		#response = urllib2.urlopen(req)
		#result = response.read()
		'''

		try:

			r = self.jsonChecker(urllib2.urlopen(url).read())
			return r
		except Exception, e:
			if e:
				if hasattr(e, 'code'):
					print "Error: HTTP GET: " + str(e.code)
			if retry == 3:
				if self.debug == True:
					print "DEBUG: Tried 3 times could not execute: GET"				
				if e:
					if hasattr(e, 'code'):
						if self.debug == True:
							print "DEBUG: HTTP CODE ON: GET - "+ str(e.code)	
						return e.code
					else:
						return False
			sleep(0.02)
			return self.httpGet(url,retry+1)
	
	def httpPut(self,url='',data={},retry=0):
		try:
			opener = urllib2.build_opener(urllib2.HTTPHandler)
			request = urllib2.Request(url, data=data)
			request.add_header('Content-Type', 'application/json')
			request.get_method = lambda: 'PUT'
			return opener.open(request)
		except Exception, e:
			if e:
				if hasattr(e, 'code'):
					print "Error: HTTP PUT: " + str(e.code)
			if retry == 3:
				if self.debug == True:
					print "DEBUG: Tried 3 times could not execute: PUT"				
				if e:
					if hasattr(e, 'code'):
						if self.debug == True:
							print "DEBUG: HTTP CODE ON: PUT - "+ str(e.code)	
						return e.code
					else:
						return False
			sleep(0.02)
			return self.httpPut(url,data,retry+1)

	def httpPost(self,url='',data={},retry=0):
		try:
			opener = urllib2.build_opener(urllib2.HTTPHandler)
			request = urllib2.Request(url, data=data)
			request.add_header('Content-Type', 'application/json')
			request.get_method = lambda: 'POST'
			return opener.open(request)
		except Exception, e:
			if e:
				if hasattr(e, 'code'):
					print "Error: HTTP POST: " + str(e.code)
			if retry == 3:
				if self.debug == True:
					print "DEBUG: Tried 3 times could not execute: POST"				
				if e:
					if hasattr(e, 'code'):
						if self.debug == True:
							print "DEBUG: HTTP CODE ON: POST - "+ str(e.code)	
						return e.code
					else:
						return False
			sleep(0.02)
			return self.httpPost(url,data,retry+1)
		
	def httpDelete(self,url='',retry=0):
		try:
			opener = urllib2.build_opener(urllib2.HTTPHandler)
			req = urllib2.Request(url, None)
			req.get_method = lambda: 'DELETE'  # creates the delete method
			return self.jsonChecker(urllib2.urlopen(req))
		except Exception, e:
			if e:
				if hasattr(e, 'code'):
					print "Error: HTTP DELETE: " + str(e.code)
			if retry == 3:
				if self.debug == True:
					print "DEBUG: Tried 3 times could not execute: DELETE"				
				if e:
					if hasattr(e, 'code'):
						if self.debug == True:
							print "DEBUG: HTTP CODE ON: DELETE - "+ str(e.code)
						return e.code
					else:
						return False
			#sleep(0.05)
			return self.httpDelete(url,retry+1)

	def jsonChecker(self, data=''):
		#checks if its good json and if so return back Python Dictionary
		try:
			checkedData = json.loads(data)
			return checkedData
		except Exception, e:
			return False

	##--------Common Methods END---------##

	def getLocalChkpt(self):
		url = self.secure +'://'+self.hostname+":"+self.port+"/"+self.sgDb+"/_local/PULL::Conflict::"+self.chkpt
		if self.debug == True:  
			print "DEBUG: CheckPointGET: "+url
		response = self.httpGet(url)
		if response == 404:
			return None
		else: 
			return response
	
	def putLocalChkpt(self,data={},rev_chkPt=''):
		if rev_chkPt == '':
			url = self.secure +'://'+self.hostname+":"+self.port+"/"+self.sgDb+"/_local/PULL::Conflict::"+self.chkpt	
		else:
			url = self.secure +'://'+self.hostname+":"+self.port+"/"+self.sgDb+"/_local/PULL::Conflict::"+self.chkpt+"?rev="+rev_chkPt
		if self.debug == True:
			print "DEBUG: CheckPointPUT: "+url
		response = self.httpPut(url,json.dumps(data))
		if response == 404 or response == 409:
			return None
		else: 
			return True

	def getChangesFeed(self,seq='0'):
		url_param = "?since="+seq+"&active_only=true&style=all_docs"
		url = self.secure +'://'+self.hostname+":"+self.port+"/"+self.sgDb+"/_changes"+url_param
		if self.debug == True:
			print "DEBUG: ChangesFeedUrl: "+url		
		response = self.httpGet(url)
		if self.debug == True:
			print "DEBUG: ChangesFeedResponse: "+str(response)
		return response

	def findConflict(self,data_json = []):
		doc_id = data_json["id"]
		winner = data_json["changes"][0]
		losers = data_json["changes"][1:]

		if self.debug == True:
				print "DEBUG: Winner:"+" DocId: "+doc_id+":"+str(winner)
				print "DEBUG: Losers:"+" DocId: "+doc_id+":"+str(losers)

		if losers.__len__() == 1 : # if only one loser just do simple DELETE

			url = self.secure +'://'+self.hostname+":"+self.port+"/"+self.sgDb+"/"+doc_id+"?rev="+losers[0]["rev"]
			if self.debug == True:
				print "DEBUG: Doc To Delete: "+url
			#sleep(0.02)
			return self.httpDelete(url) 
		
		elif losers.__len__() > 1: # if two or more loser do bulk_docs i.e.(POST)
			send_json = {};
			send_json["new_edits"] = True
			send_json["docs"] = []
			url = self.secure +'://'+self.hostname+":"+self.port+"/"+self.sgDb+"/_bulk_docs"
			for x in losers:	
				new_doc = {}
				new_doc["_id"] = doc_id
				new_doc["_rev"] = x["rev"]
				new_doc["_deleted"] = True
				send_json["docs"].append(new_doc)
			data = json.dumps(send_json)
			if self.debug == True:
				print "DEBUG: Bulk Docs Url: "+url
				#print "DEBUG: Bulk Docs Data: "+ data
			#sleep(0.05)
			return self.httpPost(url,data)

		
	#------END OF CLASS WORK -------#

if __name__ == "__main__":
	
	config["chkPt"] = str(sys.argv[1]) 
	
	a = WORK(config)

	chkPtData = a.getLocalChkpt() #get the checkpoint 

	if chkPtData == False:
		print "Error: I think SG is down"
		quit()

	chkPtAlready = False
	revChkpt = ''
	if chkPtData == None:
		feedData = a.getChangesFeed('0') #start Changes feed from ZERO (No Checkpoint)
	else:
		chkPtAlready = True #start Changes feed from checkpoint
		feedData = a.getChangesFeed(chkPtData["seq"])
		
	newSeqNum = str(feedData["last_seq"]) #get the latest sequence number from Sync Gateway _changes feed

	if chkPtAlready == True and chkPtData["seq"] == newSeqNum:#check to see if there is any changes. If not quit
		print 'No changes in sequence since last checkpoint , seq: ' + chkPtData["seq"] + " at " + chkPtData["dt"]
		quit()

	#loop through results and find conflicts
	if feedData["results"].__len__() > 0:  
		for x in feedData["results"]:
			if x["changes"].__len__() > 1:
				print "Conflicts on DocId: "+ x["id"]
				a.findConflict(x)
	
	newChkPtDt = str(datetime.datetime.now())
	
	if chkPtAlready == True: #if there is a checkpoint already I need to get _rev of it.
		revChkpt = chkPtData["_rev"]
		chkPtData["dt"] = newChkPtDt
		chkPtData["seq"] = newSeqNum
	else:
		chkPtData={"dt":newChkPtDt,"seq":newSeqNum} #first time making checkpoint

	a.putLocalChkpt(chkPtData,revChkpt) # set checkpoint
	print "Any conflicts resolved at:", datetime.datetime.now()
