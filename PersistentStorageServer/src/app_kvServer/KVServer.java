package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class KVServer  extends Thread  {
	
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
	private static Logger logger = Logger.getRootLogger();
	private int port;
	private ServerSocket serverSocket;
	private boolean running;
	private KVCache kvCache;
	
	public KVServer(int port, int cacheSize, String strategy) {
		this.port= port;
		this.kvCache = new KVCache(cacheSize, strategy);
	}
	
	
	public void run() {
		running = initializeServer();
		if (serverSocket != null) {
			while (isRunning()) {
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection = new ClientConnection(client,
							this);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+ " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! "
							+ "Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	private synchronized boolean isRunning() {
		return this.running;
	}

	/**
	 * Stops the server insofar that it won't listen at the given port any more.
	 */
	public synchronized void stopServer() {
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " + "Unable to close socket on port: " + port,
					e);
		}
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
				new KVServer(port,cacheSize,strategy).start();
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

	public KVCache getKVCache() {
		return kvCache;
	}

	public void setKVCache(KVCache kvStore) {
		this.kvCache = kvStore;
	}
}