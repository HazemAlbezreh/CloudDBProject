package app_kvServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

//import logger.LogSetup;

//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;

import config.ServerInfo;
import consistent_hashing.CommonFunctions;
import consistent_hashing.Range;
import app_kvServer.ServerStatus;

public class KVServer extends Thread  {
	
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 */
	
	///////////////////////////////////////////////////////////////////////////////
	
	private KVCache kvCache=null;
	//private static Logger logger = Logger.getRootLogger();
	private ServerSocket serverSocket=null;
	private int port;
	
	///////////////////////////////////////////////////////////////////////////////
	private boolean running;
	private ServerStatus serverStatus;
	private List<ClientConnection> activeThreads;
	private SortedMap<Integer, ServerInfo> ring=null;
	private Range range=null;
	
	///////////////////////////////////////////////////////////////////////////////

	List<ServerInfo> replicas= new ArrayList<ServerInfo>();
	private Range repRange=null;
	UpdateTimer timer=null;
	
	///////////////////////////////////////////////////////////////////////////////
	
	public KVServer(int port, int cacheSize, String strategy, String datasetName, String replicaName) {
		this.port= port;
		this.kvCache = new KVCache(String.valueOf(port),cacheSize, strategy, datasetName, replicaName,false);
		this.activeThreads=new ArrayList<ClientConnection>();
		this.serverStatus=ServerStatus.STOPPED;
		this.ring=null;
		this.start();
	}
	
	public KVServer(int port) {
		this.port= port;
		this.activeThreads=new ArrayList<ClientConnection>();
		this.serverStatus=ServerStatus.INIT;
		this.ring=new TreeMap<Integer,ServerInfo>();
		this.start();
	}
	
	public void run() {
		running = initializeServer();
		if (serverSocket != null) {
			while (isRunning()) {
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection = new ClientConnection(client,this);
					this.addThread(connection);
					new Thread(connection).start();
					//logger.info("Connected to " + client.getInetAddress().getHostName()+ " on port " + client.getPort());
				} catch (IOException e) {
					if(isRunning()){
					//	logger.error("Error! Unable to establish connection. \n", e);
					}else{
					//	logger.info("Shutdown server");
					}					
				}
			}
		}
	//	logger.info("Server stopped.");
	}

	private synchronized boolean isRunning() {
		return (this.serverStatus!=ServerStatus.SHUTDOWNED);
	}

	private boolean initializeServer() {
		//logger.info("Initialize server ...");
		System.out.println("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
		//	logger.info("Server listening on port: "
			//		+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
//			logger.error("Error! Cannot open server socket:");
//			if (e instanceof BindException) {
//				logger.error("Port " + port + " is already bound!");
//			}
			return false;
		}
	}

	/**
	 * Main entry point for the echo server application.
	 * 
	 * @param args
	 *            contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
		//	Level loglevel = Level.ALL;
			switch (args.length) {
			case 4:
		//		loglevel = Level.toLevel(args[3]);
				break;
			case 3:
				int port = Integer.parseInt(args[0]);
				int cacheSize= Integer.parseInt(args[1]);
				String strategy= args[2];
		//		new LogSetup("logs/server" + port + ".log", loglevel);
				new KVServer(port,cacheSize,strategy,"dataset","replica"); //.start();
				break;
			case 1:
				int port2 = Integer.parseInt(args[0]);
			//	new LogSetup("logs/server" + port2 + ".log", loglevel);
				new KVServer(port2);
				break;
			default:
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
				break;
			}
//		} catch (IOException e) {
//			System.out.println("Error! Unable to initialize logger!");
//			e.printStackTrace();
//			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}

	public synchronized KVCache getKVCache() {
		return kvCache;
	}

	public synchronized void setKVCache(KVCache kvStore) {
		this.kvCache = kvStore;
	}
	
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////	
	
	public synchronized ServerStatus getStatus(){
		return this.serverStatus;	
	}
	
	public synchronized void setStatus(ServerStatus status){
		this.serverStatus=status;	
	}
	
	public synchronized SortedMap<Integer,ServerInfo> getMetadata(){
		return this.ring;
	}
	public synchronized void setMetadata(SortedMap<Integer,ServerInfo> data){
		this.ring=data;
	}
	
	public synchronized boolean initKVServer(int cacheSize, String strategy,SortedMap<Integer,ServerInfo> data,Range range,Range rep){
		this.kvCache=new KVCache(String.valueOf(port), cacheSize, strategy,"dataset","replica",false);
		this.setMetadata(data);
		this.setRange(range);
		this.setReplicaRange(rep);
		this.setReplicas( this.findReplicas(data, range) );
//		this.timer=new UpdateTimer(this,10);
		
		if(this.getMetadata()==null || this.getRange()==null || 
				this.getKVCache()==null || this.getStatus()!=ServerStatus.INIT){
			return false;
		}
		this.setStatus(ServerStatus.STOPPED);
		return true;
	}
	
	public synchronized boolean stopServer(){
		if(this.getStatus()!=ServerStatus.STARTED){
			return false;
		}
		this.setStatus(ServerStatus.STOPPED);
		return true;
	}
	
	public synchronized boolean lockWrite(){
		if(this.getStatus()!=ServerStatus.STARTED){
			return false;
		}
		this.setStatus(ServerStatus.LOCKED);
		return true;	
	}
	
	public synchronized boolean unlockWrite(){
		if(this.getStatus()!=ServerStatus.LOCKED){
			return false;
		}
		this.setStatus(ServerStatus.STARTED);
		return true;
	}
	
	public synchronized void update (SortedMap<Integer,ServerInfo> data,Range r,Range rep){
		this.setMetadata(data);
		this.setRange(r);
		this.setReplicas( this.findReplicas(data, r) );
		this.setReplicaRange(rep);
		
	}
	
	public synchronized void shutDown(){
		//logger.info("Initiating Shutdown of server");
		this.setStatus(ServerStatus.SHUTDOWNED);
		try {
			serverSocket.close();		
			this.timer.cancelTimer();
			for(Iterator<ClientConnection> i = this.activeThreads.iterator(); i.hasNext();) {
				ClientConnection clientThread=i.next();
				try{
					clientThread.terminateThread();
				}catch (IOException e) {
				//	logger.error("Error! " + "Terminate thread exception ");
				}
				i.remove();
			}

		} catch (IOException e) {
		//	logger.error("Error! " + "Unable to close socket on port: " + port,e);
		}
	}

	public synchronized boolean startServer(){
		if(this.getMetadata()==null || this.getRange()==null || 
				this.getKVCache()==null || (this.getStatus()!=ServerStatus.STOPPED && this.getStatus()!=ServerStatus.STARTED) ){
			return false;
		}
		this.setStatus(ServerStatus.STARTED);
		return true;
	}

	public synchronized void setRange(Range r){
		this.range=r;
	}
	
	public synchronized Range getRange(){
		return this.range;
	}
	
	public synchronized boolean inRange(int i) {
		return this.range.isWithin(i);
	}

	public synchronized void addThread(ClientConnection cc){
		this.activeThreads.add(cc);
	}
	
	public synchronized void removeThread(ClientConnection cc){
		if(this.activeThreads.contains(cc)){
			this.activeThreads.remove(cc);
		}
	}
	
	public synchronized List<ServerInfo> getReplicas(){
		return this.replicas;
	}
	
	public synchronized void setReplicas(List<ServerInfo> re){
		this.replicas=re;
	}
	
	synchronized List<ServerInfo> findReplicas(SortedMap<Integer,ServerInfo> data,Range r){
		int serverKey = r.getHigh();
		ServerInfo currentServer = data.get(serverKey);

		List<ServerInfo> li=new ArrayList<ServerInfo>();

		if(data.size()==2){
			ServerInfo newSuccessor = CommonFunctions.getSuccessorNode(currentServer, data);
			li.add(newSuccessor);
			
		}else if(data.size()>=3){
			ServerInfo newSuccessor = CommonFunctions.getSuccessorNode(currentServer, data);
			ServerInfo newSecondSuccessor = CommonFunctions.getSecondSuccessorNode(currentServer, data);
			
			li.add(newSuccessor);
			li.add(newSecondSuccessor);
		}
		
		return li;
	}
	
	public synchronized void setReplicaRange(Range r){
		this.repRange=r;
	}
	
	public synchronized Range getReplicaRange(){
		return this.repRange;
	}
	
	public synchronized boolean inReplicaRange(int i) {
		return this.repRange.isWithin(i);
	}

	public synchronized void addToTimer(String key,String value){
		this.timer.addData(key, value);
	}
}