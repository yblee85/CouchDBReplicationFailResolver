# CouchDBReplicationFailResolver (Java)
when there's a active replication that is stuck at certain seq number, 
it will find the doc from the source db via _chagnes and increase rev by updating the doc

Tested
Client couch 1.2.2
Server couch 1.3.1 / 1.6.1
Java 1.7
