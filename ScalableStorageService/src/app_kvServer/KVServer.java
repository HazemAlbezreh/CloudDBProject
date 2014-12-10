package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import config.ServerInfo;
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
	private static Logger logger = Logger.getRootLogger();
	private ServerSocket serverSocket=null;
	private int port;
	
	///////////////////////////////////////////////////////////////////////////////
	private boolean running;
	
	private ServerStatus serverStatus;
	private List<ClientConnection> activeThreads;
	private SortedMap<Integer, ServerInfo> ring=null;
	private Range range=null;;
	
	///////////////////////////////////////////////////////////////////////////////
	
	public KVServer(int port, int cacheSize, String strategy) {
		this.port= port;
		this.kvCache = new KVCache(String.valueOf(port),cacheSize, strategy);
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
					this.activeThreads.add(connection);
					new Thread(connection).start();
					logger.info("Connected to " + client.getInetAddress().getHostName()+ " on port " + client.getPort());
				} catch (IOException e) {
					if(isRunning()){
						logger.error("Error! Unable to establish connection. \n", e);
					}else{
						logger.info("Shutdown server");
					}					
				}
			}
		}
		logger.info("Server stopped.");
	}

	private synchronized boolean isRunning() {
		return (this.serverStatus!=ServerStatus.SHUTDOWNED);
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound!");
			}
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
			Level loglevel = Level.ALL;
			switch (args.length) {
			case 4:
				loglevel = Level.toLevel(args[3]);
			case 3:
				int port = Integer.parseInt(args[0]);
				int cacheSize= Integer.parseInt(args[1]);
				String strategy= args[2];
				new LogSetup("logs/server" + port + ".log", loglevel);
				new KVServer(port,cacheSize,strategy); //.start();
				break;
			case 1:
				int port2 = Integer.parseInt(args[0]);
				new LogSetup("logs/server" + port2 + ".log", loglevel);
				new KVServer(port2);
				break;
			default:
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
				break;
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
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
	
	public synchronized boolean initKVServer(int cacheSize, String strategy,SortedMap<Integer,ServerInfo> data,Range range){
		this.kvCache=new KVCache(String.valueOf(port), cacheSize, strategy);
		this.setMetadata(data);
		this.setRange(range);
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
	
	public synchronized void update (SortedMap<Integer,ServerInfo> data,Range r){
		this.setMetadata(data);
		this.setRange(r);
	}
	
	public synchronized void shutDown(){
		logger.info("Initiating Shutdown of server");
		this.setStatus(ServerStatus.SHUTDOWNED);
		try {
			serverSocket.close();
			for(ClientConnection cc : this.activeThreads){
				try{
					cc.terminateThread();
				}catch (IOException e) {
					logger.error("Error! " + "Terminate thread exception ");
				}
			}
		} catch (IOException e) {
			logger.error("Error! " + "Unable to close socket on port: " + port,e);
		}
	}

	public synchronized boolean startServer(){
		if(this.getMetadata()==null || this.getRange()==null || 
				this.getKVCache()==null || this.getStatus()!=ServerStatus.STOPPED){
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


}