# CouchDBReplicationFailResolver
when there's a active replication that is stuck at certain seq number, it will find the doc from the source db via _chagnes and increase rev by updating the doc
