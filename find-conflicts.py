#!/usr/bin/python
import json,urllib2,time,datetime

config = {"hostname":"127.0.0.1","port":"4985","sgDb":"sync_gateway","debug":True}

class WORK():

	hostname = '127.0.0.1'
	port = '4985'
	sgDb = 'db'
	debug = False

	def __init__(self,config):
		self.hostname = config["hostname"]
		self.port = config["port"]
		self.sgDb = config["sgDb"]
		self.debug = config["debug"]

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
		
	def httpDelete(self,url='',retry=0):

		try:
			#url = "http://127.0.0.1:4985/sync_gateway/fujio?rev=3-53a4c0483d49b40ded0ff95dad890540"
			opener = urllib2.build_opener(urllib2.HTTPHandler)
			req = urllib2.Request(url, None)
			req.get_method = lambda: 'DELETE'  # creates the delete method
			r = self.jsonChecker(urllib2.urlopen(req))
			return r
		except Exception, e:
			if retry == 3:
				if self.debug == True:
					return None
					print "Debug:HTTPGetError:"+str(e.code)
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
		url = 'http://'+self.hostname+":"+self.port+"/"+self.sgDb+"/_local/PULL::Conflict::"+"fujio"
		if self.debug == True:  
			print "Debug:CheckPointGET: "+url
		response = self.httpGet(url)
		return response
	
	def putLocalChkpt(self,data={},rev_chkPt=''):

		if rev_chkPt == '':
			url = 'http://'+self.hostname+":"+self.port+"/"+self.sgDb+"/_local/PULL::Conflict::"+"fujio"	
		else:
			url = 'http://'+self.hostname+":"+self.port+"/"+self.sgDb+"/_local/PULL::Conflict::"+"fujio"+"?rev="+rev_chkPt
		if self.debug == True:
			print "Debug:CheckPointPUT: "+url

		self.httpPut(url,json.dumps(data))
		return True


	def bulkGet(self,doc_id='',revs=[]):
		docs_back = []
		for x in revs:
			#print x
			url = 'http://'+self.hostname+":"+self.port+"/"+self.db+"/"+doc_id+"?rev="+x["rev"]
			#r = urllib2.urlopen('http://localhost:4984/sync_gateway/'+doc_id+"?rev="+x["rev"]).read()
			#docs_back.append(json.loads(r))
		return docs_back

	def bulkPut(self,data = {}):
		print data


	def getChangesFeed(self,seq='0'):

		url_param = "?since="+seq+"&active_only=true&style=all_docs"
		url = 'http://'+self.hostname+":"+self.port+"/"+self.sgDb+"/_changes"+url_param
		if self.debug == True:
			print "Debug:ChangesFeedUrl:"+url		
		response = self.httpGet(url)
		if self.debug == True:
			print "Debug:ChangesFeedResponse:"+str(response)
		return response


	def findConflict(self,data_json = []):
		#print data_json
		#return True
		doc_id = data_json["id"]
		winner = data_json["changes"][0]
		losers = data_json["changes"][1:]

		if self.debug == True:
				print "Debug:Winner:"+" DocId: "+doc_id+":"+str(winner)
				print "Debug:Losers:"+" DocId: "+doc_id+":"+str(losers)
		for x in losers: #delete Losers
			url = 'http://'+self.hostname+":"+self.port+"/"+self.sgDb+"/"+doc_id+"?rev="+x["rev"]
			if self.debug == True:
				print "Debug:Doc To Delete:"+url
			self.httpDelete(url) 

	#------END OF CLASS WORK -------#

if __name__ == "__main__":
	a = WORK(config)

	#get a checkpoint
	chkPtData = a.getLocalChkpt()

	chkPtAlready = False
	revChkpt = ''
	if chkPtData == None:
		#start Changes feed from ZERO (No Checkpoint)
		feedData = a.getChangesFeed('0')
	else:
		#start Changes feed from checkpoint
		chkPtAlready = True
		feedData = a.getChangesFeed(chkPtData["seq"])
		
	newSeqNum = str(feedData["last_seq"]) #latest sequence number from Sync Gateway

	#check to see if there is any changes. If not Return
	if chkPtAlready == True and chkPtData["seq"] == newSeqNum:
		print 'No changes in sequence since last checkpoint , seq: ' + chkPtData["seq"] + " at " + chkPtData["dt"]
		quit()

	#loop through results and find conflicts
	if feedData["results"].__len__() > 0:  
		for x in feedData["results"]:
			if x["changes"].__len__() > 1:
				print "Conflicts on DocId: "+ x["id"]
				a.findConflict(x)
	
	newChkPtDt = str(datetime.datetime.now())
	#if there is a checkpoint alread I need to get _rev of it.
	if chkPtAlready == True:
		revChkpt = chkPtData["_rev"]
		chkPtData["dt"] = newChkPtDt
		chkPtData["seq"] = newSeqNum
	else:
		chkPtData={"dt":newChkPtDt,"seq":newSeqNum}

	a.putLocalChkpt(chkPtData,revChkpt) # set checkpoint
	print "Any conflicts resolved at:", datetime.datetime.now()
