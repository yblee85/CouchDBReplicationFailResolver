package couchdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import javax.swing.SwingUtilities;

import org.apache.commons.io.IOUtils;
import org.apache.http.conn.ConnectTimeoutException;
import org.codehaus.jackson.JsonNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;
import org.json.JSONArray;
import org.json.JSONObject;

public class CouchDBReplicationFailResolver {
	private String local_couchdb_address, server_couchdb_address;
	private String local_couchdb_admin_user, local_couchdb_admin_pass, server_couchdb_admin_user, server_couchdb_admin_pass;
	private Timer timer;
	private int connection_timeout, threshold_time, repeat_duration, buffer_time;
	private boolean enabledResolver = false, isUpdateDesignDoc = false;
	private String INI_FILE_NAME = "couchdb_replication_fail_resolve.ini";
	
	private Properties resolveIni;
	
	public CouchDBReplicationFailResolver(String localAddress, String local_user, String local_pass, 
											String serverAddres, String server_user, String server_pass,
											boolean isUpdateDesignDoc) {
		local_couchdb_address = localAddress;
		server_couchdb_address = serverAddres;
		
		local_couchdb_admin_user = local_user;
		local_couchdb_admin_pass = local_pass;
		server_couchdb_admin_user = server_user;
		server_couchdb_admin_pass = server_pass;
		
		this.isUpdateDesignDoc = isUpdateDesignDoc;
		
		// this setup can be set from outside
		connection_timeout = 5000;					// 5 sec
		threshold_time = 1000 * 60 * 10;			// 10 min : if the seq hasn't been changed for this amount, it will trigger resolver
		repeat_duration = 1000 * 60 * 10;			// 10 min
		buffer_time = 1000 * 30;					// 30 sec
	}
	
	public boolean isEnabledResolver() {
		return enabledResolver;
	}

	public void setEnabledResolver(boolean enabledResolver) {
		this.enabledResolver = enabledResolver;
		
		if(timer != null) {
			try {
				timer.cancel();
				timer.purge();
				timer = null;
			} catch(Exception e) {}
		}
		
		if(enabledResolver) {
			timer = new Timer();
			timer.scheduleAtFixedRate(new TimerTask() {
				@Override
				public void run() {
					System.out.println("Replication Resolver timer.");
					resolveIssues();
				}
			}, 0, repeat_duration);
		}
		
		
	}

	private void resolveIssues() {
		boolean isLoaded = loadResolveIni();
		
		JSONArray oldListActiveTasks = null;
		JSONArray newlistActiveTasks = getLocalCouchDBInProgressReplications(local_couchdb_address, local_couchdb_admin_user, local_couchdb_admin_pass);
		String strTimeStamp = resolveIni.getProperty("timestamp");
		if(strTimeStamp != null && !strTimeStamp.isEmpty()) {
			String strListActiveTasks = resolveIni.getProperty("in_progress_replications");
			oldListActiveTasks = new JSONArray(strListActiveTasks);
		}
		
		Date now = new Date();
		resolveIni.setProperty("timestamp", now.getTime()+"");
		resolveIni.setProperty("in_progress_replications", newlistActiveTasks.toString());
		boolean isStored = saveResolveIni();
		
		if(oldListActiveTasks != null) {
			int diff = Math.abs((int)(now.getTime() - Long.parseLong(strTimeStamp)));
			if(diff >= (threshold_time - buffer_time)) {
				JSONArray listFailedTasks = getFailedReplicationTasks(newlistActiveTasks, oldListActiveTasks);
				if(listFailedTasks != null && listFailedTasks.length() > 0) {
					// work in background to resolve issue
					for(int index=0; index<listFailedTasks.length(); index++) {
						final JSONObject failedTask = listFailedTasks.getJSONObject(index);
						try {
							SwingUtilities.invokeAndWait(new Runnable() {
								@Override
								public void run() {
									_resolveFaildReplication(failedTask);
								}
							});
							System.out.println("Task Resolve Thread excuted, index : " + index);
						} catch (InvocationTargetException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
	}
	
	private void _resolveFaildReplication(JSONObject failedTask) {
		long seq = failedTask.getLong("checkpointed_source_seq");
		String source_raw = failedTask.getString("source");
		
		boolean isServer = source_raw.contains(server_couchdb_admin_user + ":");
		String strPrefix = source_raw.substring(0, source_raw.indexOf("@")+1);
		String str_ip_db = source_raw.replace(strPrefix, "");
		String str_ip = str_ip_db.substring(0, str_ip_db.indexOf("/"));
		String str_db = str_ip_db.replace(str_ip, "").replace("/", "");
		
		System.out.println("ip : " + str_ip + ", db : " + str_db);
		
		// query _changes
		HttpClient httpClient=null;
		try {
			String username = server_couchdb_admin_user;
			String password = server_couchdb_admin_pass;
			if(!isServer) {
				username = local_couchdb_admin_user;
				password = local_couchdb_admin_pass;
			}
			
			httpClient = new StdHttpClient.Builder().connectionTimeout(connection_timeout).socketTimeout(connection_timeout).url("http://" + str_ip).username(username).password(password).build();
			
			String str = "";
			java.io.InputStream is = null;
			try {
				String strParam = "since=" + (seq-1) + "&limit=1";
				is = httpClient.get("/" + str_db + "/_changes?" + strParam).getContent();
				StringWriter writer = new StringWriter();
				IOUtils.copy(is, writer);
				str = writer.toString();
				is.close();
				is=null;
				
				JSONObject resp = new JSONObject(str);
				JSONArray results = resp.getJSONArray("results");
				if(results != null && results.length()>0) {
					JSONObject changeObj = results.getJSONObject(0);
					long foundSeq = changeObj.getLong("seq");
					boolean isDeleted = changeObj.isNull("deleted")?false:Boolean.parseBoolean(changeObj.getString("deleted"));
					if(foundSeq == seq && !isDeleted) {
						String failedDocId = changeObj.getString("id");
						System.out.println("found the docid that has issue : " + failedDocId);
						
						if(failedDocId.startsWith("_design") && !isUpdateDesignDoc) {
							System.out.println("It's a design doc, It wont' update this design doc.");
						} else {
							// fetch doc
							try {
								CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
								CouchDbConnector dbConn = new StdCouchDbConnector(str_db, dbInstance);
								JsonNode docObj = dbConn.get(JsonNode.class, failedDocId);
								
								// update doc
								dbConn.update(docObj);
								
								System.out.println("document that has an issue has been updated");
							} catch(Exception e) {
								e.printStackTrace();
							}
						}
						
					} else {
						System.out.println("document that has an issue could not be updated, task obj : " + changeObj.toString());
					}
				}
			} catch(ConnectTimeoutException e) {
				// Server not rechable?
				System.out.println("============== ReplicationFailResolver : Server seems to be unable to reach ===============");
				setEnabledResolver(false);
			} catch (IOException e) {
				e.printStackTrace();
			}  catch(Exception e) {
				e.printStackTrace();
			} finally {
				if(is!=null) {
					try {
						is.close();
						is=null;
					} catch(IOException e) {
					}
				}
			}
			
			httpClient.shutdown();
		} catch (MalformedURLException e1) {
		} catch (Exception e1) {}
		
	} 
	
	private boolean loadResolveIni() {
		if(resolveIni == null) {
			resolveIni = new Properties();
		}
		
		File file = new File(INI_FILE_NAME);
		FileReader reader;
		try {
			reader = new FileReader(file);
			resolveIni.load(reader);
			System.out.println("replication fail resolver ini loaded");
			return true;
		} 
		catch (FileNotFoundException e) { 
		} catch (IOException e) {
			e.printStackTrace();
		} catch(Exception e) { 
			e.printStackTrace(); 
		}
		return false;		
	} 
	
	private boolean saveResolveIni(){
		FileOutputStream outputFile = null;
		try{
			outputFile = new FileOutputStream(INI_FILE_NAME);
			resolveIni.store(outputFile, "Update");
			System.out.println("replication fail resolver ini saved");
		}catch(Exception ex){
			ex.printStackTrace();
			return false;
		} finally{
			if(outputFile!=null) try { outputFile.close(); } catch (IOException e) {}
		}
		return true;
	}
	
	private JSONArray getLocalCouchDBActiveTasks(String localAddress, String local_couchdb_admin_user, String local_couchdb_admin_pass) {
		JSONArray listActiveTasks = null;
		HttpClient httpClient=null;

		try {
			httpClient = new StdHttpClient.Builder().connectionTimeout(connection_timeout).socketTimeout(connection_timeout).url(localAddress).username(local_couchdb_admin_user).password(local_couchdb_admin_pass).build();
			
			String str = "";
			java.io.InputStream is = null;
			try {
				is = httpClient.get("/_active_tasks").getContent();
				StringWriter writer = new StringWriter();
				IOUtils.copy(is, writer);
				str = writer.toString();
				
				listActiveTasks = new JSONArray(str);
				
				is.close();
				is=null;
			} catch (IOException e) {
			} finally {
				if(is!=null) {
					try {
						is.close();
						is=null;
					} catch(IOException e) {
					}
				}
			}
			
			httpClient.shutdown();
		} catch (MalformedURLException e1) {
		} catch (Exception e1) {}
		
		return listActiveTasks;
	}
	
	private JSONArray getLocalCouchDBInProgressReplications(String localAddress, String local_couchdb_admin_user, String local_couchdb_admin_pass) {
		JSONArray listActiveTasks = getLocalCouchDBActiveTasks(localAddress, local_couchdb_admin_user, local_couchdb_admin_pass);
		if(listActiveTasks == null) return null;
		
		JSONArray listInProgressTasks = new JSONArray();
		try {
			for(int index=0; index<listActiveTasks.length(); index++) {
				JSONObject tmpTask = listActiveTasks.getJSONObject(index);
				String taskType = tmpTask.getString("type");
				int progress = tmpTask.getInt("progress");
				long checkpointed_src_seq = tmpTask.getLong("checkpointed_source_seq");
				long current_src_seq = tmpTask.getLong("source_seq");
				if(taskType.equals("replication") && checkpointed_src_seq>0 && checkpointed_src_seq!=current_src_seq) {
					System.out.println("In Progress Replication doc_id : " + tmpTask.getString("doc_id") + ", replication_id : " + tmpTask.getString("replication_id"));
					listInProgressTasks.put(tmpTask);
				}
			}
			
			return listInProgressTasks;
		} catch(Exception e) {
			return null;
		}
	}
	
	private JSONArray getFailedReplicationTasks(JSONArray listNew, JSONArray listOld) {
		JSONArray listResult = null;
		
		if(listNew != null && listOld != null) {
			listResult = new JSONArray();
			Map<String, JSONObject> mapOldTask = new HashMap<String, JSONObject>();
			for(int index=0; index<listOld.length(); index++) {
				JSONObject tmpTask = listOld.getJSONObject(index);
				mapOldTask.put(tmpTask.getString("replication_id"), tmpTask);
			}
			
			for(int index=0; index<listNew.length(); index++) {
				JSONObject tmpNewTask = listNew.getJSONObject(index);
				String replication_id = tmpNewTask.getString("replication_id");
				JSONObject tmpOldTask = mapOldTask.get(replication_id);
				if(tmpOldTask != null) {
					long new_chk_src_seq = tmpNewTask.getLong("checkpointed_source_seq");
					long old_chk_src_sep = tmpOldTask.getLong("checkpointed_source_seq");
					if(new_chk_src_seq == old_chk_src_sep) {
						System.out.println("Failed Replication doc_id : " + tmpNewTask.getString("doc_id") + ", replication_id : " + tmpNewTask.getString("replication_id"));
						listResult.put(tmpNewTask);
					}
				}
			}
		}
		
		return listResult;
	}
}
