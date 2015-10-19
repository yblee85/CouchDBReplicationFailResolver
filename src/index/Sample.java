package index;

import couchdb.CouchDBReplicationFailResolver;

public class Sample {
	public static void main(String[] args) {
		String localAddress = "local_couchdb_ip:portnumber";
		String localUsername = "local_user";
		String localPassword = "local_pass";
				
		String serverAddress = "server_couchdb_ip:portnumber";
		String serverUsername = "server_user";
		String serverPassword = "server_pass";
		
		CouchDBReplicationFailResolver resolver = new CouchDBReplicationFailResolver(localAddress, localUsername, localPassword, 
																						serverAddress, serverUsername, serverPassword,
																						false);
		resolver.setEnabledResolver(true);
		
		boolean isResolverEnabled = true;
		while(isResolverEnabled) {
			try {
				Thread.sleep(1000 * 60 * 10);		// sleep for 10 min
				isResolverEnabled = resolver.isEnabledResolver();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
