#!/usr/bin/python
import json,urllib2,time,datetime

config = {"hostname":"127.0.0.1","port":"4985","sgDb":"sync_gateway","secure":False,"debug":True}

class WORK():

	hostname = '127.0.0.1'
	port = '4985'
	sgDb = 'db'
	debug = False
	secure = "http"
	chkpt ='name_space'

	def __init__(self,config):
		self.hostname = config["hostname"]
		self.port = config["port"]
		self.sgDb = config["sgDb"]
		self.debug = config["debug"]
		if config["secure"] == True:
			self.secure = "https"
		else:
			self.secure = "http" 

	##--------Common Methods BEGIN---------##
	def sayHelloTest(self):
		print "hello"

	def httpGet(self,url='',retry=0):
		try:
			r = self.jsonChecker(urllib2.urlopen(url).read())
			return r
		except Exception, e:
			if retry == 3:
				if self.debug == True:
					return None
					print "Debug:HTTPGetError:"+str(e.code)
				return False
			self.httpGet(url,retry+1)
	
	def httpPut(self,url='',data={},retry=0):
		try:
			opener = urllib2.build_opener(urllib2.HTTPHandler)
			request = urllib2.Request(url, data=data)
			request.add_header('Content-Type', 'application/json')
			request.get_method = lambda: 'PUT'
			r = opener.open(request)
		except Exception, e:
			if retry == 3:
				if self.debug == True:
					return None
					print "Debug:HTTPPutError:"+str(e.code)
				return False
			self.httpPut(url,data,retry+1)
		return r
		
	def httpPost(self,url='',data={},retry=0):
		try:
			opener = urllib2.build_opener(urllib2.HTTPHandler)
			request = urllib2.Request(url, data=data)
			request.add_header('Content-Type', 'application/json')
			request.get_method = lambda: 'POST'
			r = opener.open(request)
		except Exception, e:
			if retry == 3:
				if self.debug == True:
					return None
					print "Debug: HTTPPostError:"+str(e.code)
				return False
			self.httpPut(url,data,retry+1)
		return r
		
	def httpDelete(self,url='',retry=0):
		try:
			opener = urllib2.build_opener(urllib2.HTTPHandler)
			req = urllib2.Request(url, None)
			req.get_method = lambda: 'DELETE'  # creates the delete method
			r = self.jsonChecker(urllib2.urlopen(req))
			return r
		except Exception, e:
			if retry == 3:
				if self.debug == True:
					return None
					print "Debug: HTTPDeleteError:"+str(e.code)
				return False
			self.httpDelete(url,retry+1)


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
			print "Debug: CheckPointGET: "+url
		response = self.httpGet(url)
		return response
	
	def putLocalChkpt(self,data={},rev_chkPt=''):
		if rev_chkPt == '':
			url = self.secure +'://'+self.hostname+":"+self.port+"/"+self.sgDb+"/_local/PULL::Conflict::"+self.chkpt	
		else:
			url = self.secure +'://'+self.hostname+":"+self.port+"/"+self.sgDb+"/_local/PULL::Conflict::"+self.chkpt+"?rev="+rev_chkPt
		if self.debug == True:
			print "Debug: CheckPointPUT: "+url
		self.httpPut(url,json.dumps(data))
		return True


	def bulkGet(self,doc_id='',revs=[]):
		docs_back = []
		for x in revs:
			#print x
			url = self.secure +'://'+self.hostname+":"+self.port+"/"+self.db+"/"+doc_id+"?rev="+x["rev"]
			#r = urllib2.urlopen('http://localhost:4984/sync_gateway/'+doc_id+"?rev="+x["rev"]).read()
			#docs_back.append(json.loads(r))
		return docs_back

	def bulkPut(self,data = {}):
		print data


	def getChangesFeed(self,seq='0'):
		url_param = "?since="+seq+"&active_only=true&style=all_docs"
		url = self.secure +'://'+self.hostname+":"+self.port+"/"+self.sgDb+"/_changes"+url_param
		if self.debug == True:
			print "Debug: ChangesFeedUrl: "+url		
		response = self.httpGet(url)
		if self.debug == True:
			print "Debug: ChangesFeedResponse: "+str(response)
		return response

	def findConflict(self,data_json = []):
		doc_id = data_json["id"]
		winner = data_json["changes"][0]
		losers = data_json["changes"][1:]

		if self.debug == True:
				print "Debug: Winner:"+" DocId: "+doc_id+":"+str(winner)
				print "Debug: Losers:"+" DocId: "+doc_id+":"+str(losers)

		if losers.__len__ == 1: # if only one loser just do simple DELETE
			url = self.secure +'://'+self.hostname+":"+self.port+"/"+self.sgDb+"/"+doc_id+"?rev="+x["rev"]
			if self.debug == True:
				print "Debug: Doc To Delete: "+url
			self.httpDelete(url) 
		elif losers.__len__ > 1: # if two or more loser do bulk_docs i.e.(POST)
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
				print "Debug: Bulk Docs Url: "+url
				print "Debug: Bulk Docs Data: "+ data
			self.httpPost(url,data)

	#------END OF CLASS WORK -------#

if __name__ == "__main__":
	a = WORK(config)

	chkPtData = a.getLocalChkpt() #get a checkpoint

	chkPtAlready = False
	revChkpt = ''
	if chkPtData == None:
		feedData = a.getChangesFeed('0') #start Changes feed from ZERO (No Checkpoint)
	else:
		chkPtAlready = True #start Changes feed from checkpoint
		feedData = a.getChangesFeed(chkPtData["seq"])
		
	newSeqNum = str(feedData["last_seq"]) #latest sequence number from Sync Gateway

	if chkPtAlready == True and chkPtData["seq"] == newSeqNum:#check to see if there is any changes. If not Return
		print 'No changes in sequence since last checkpoint , seq: ' + chkPtData["seq"] + " at " + chkPtData["dt"]
		quit()

	#loop through results and find conflicts
	if feedData["results"].__len__() > 0:  
		for x in feedData["results"]:
			if x["changes"].__len__() > 1:
				print "Conflicts on DocId: "+ x["id"]
				a.findConflict(x)
	
	newChkPtDt = str(datetime.datetime.now())
	
	if chkPtAlready == True: #if there is a checkpoint alread I need to get _rev of it.
		revChkpt = chkPtData["_rev"]
		chkPtData["dt"] = newChkPtDt
		chkPtData["seq"] = newSeqNum
	else:
		chkPtData={"dt":newChkPtDt,"seq":newSeqNum}

	a.putLocalChkpt(chkPtData,revChkpt) # set checkpoint
	print "Any conflicts resolved at:", datetime.datetime.now()