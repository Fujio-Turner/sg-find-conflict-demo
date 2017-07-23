# sg-find-conflict-demo
A simple conflict resolver for Sync Gateway


Insert into your cron tab 
```
* * * * * /usr/bin/python /path/to/sg-find-conflict-demo/sg-find-conflict.py
```

**FAQ**

Q:How does it resolves conflicts?

A:Sync Gateway uses the CouchDB method of determining which revision of a document in conflict to show.
```
CouchDB guarantees that each instance that sees the same conflict comes up with the same winning and losing revisions. It does so by running a deterministic algorithm to pick the winner. The application should not rely on the details of this algorithm and must always resolve conflicts. We’ll tell you how it works anyway.

Each revision includes a list of previous revisions. The revision with the longest revision history list becomes the winning revision. If they are the same, the _rev values are compared in ASCII sort order, and the highest wins. So, in our example, 2-de0ea16f8621cbac506d23a0fbbde08a beats 2-7c971bb974251ae8541b8fe045964219.

One advantage of this algorithm is that CouchDB nodes do not have to talk to each other to agree on winning revisions. We already learned that the network is prone to errors and avoiding it for conflict resolution makes CouchDB very robust.
```
source:http://guide.couchdb.org/draft/conflicts.html

The default shown document will be the "winner" and the script will removing(deleting) the other revisions in conflict.


Q:Can I change the logic to pick a specific winner?

A:Yes, you'll have to change the script to your needs. 

Just put in your logic here on line 164 in find-conflicts.py
```
	def findConflict(self,data_json = []):
		doc_id = data_json["id"]
		winner = data_json["changes"][0]
		losers = data_json["changes"][1:]
```

Q:How often does it run?

A:Once a minute is a good starting point. Remember it takes time for it to process the _changes feed.

Q:Does it process all the documents in the whole Sync Gateway Database every time it runs?

A: No, it process the whole _changes feed the first it run , but then it creates a checkpoint and process the changes since the last point it processed the _changes feed.

Q:Can I run the conflict finder on multiple Sync gateways?

A: Yes, but make sure they don't run at the exact same time. Example SG 8.8.8.8 every 13 seconds and SG 9.9.9.9 run every 43 seconds. Prime number intervals are a good guide line.
