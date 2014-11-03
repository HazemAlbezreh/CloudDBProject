package client;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import socket.SocketWrapper;


import client.ClientSocketListener.SocketStatus;


import common.messages.KVMSG;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.TextMessage;

public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;

	private SocketWrapper clientSocket;
	private OutputStream output;
	private InputStream input;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;


	private String address;
	private int port;
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */

	public KVStore(String address, int port)  {
		this.address=address;
		this.port=port;
		listeners = new HashSet<ClientSocketListener>();
		logger.info("Connection established");
		
	}



	@Override
	public void connect() throws IOException {
		try {
			clientSocket = new SocketWrapper();
			clientSocket.connect(this.address, this.port);
			setRunning(true);
			
			
			TextMessage latestMsg = clientSocket.receiveMessage();
			for(ClientSocketListener listener : listeners) {
				listener.handleNewMessage(latestMsg);
			}
		} catch (IOException ioe) {
			if(isRunning()) {
				logger.error("Connection lost!");
				try {
					tearDownConnection();
					for(ClientSocketListener listener : listeners) {
						listener.handleStatus(
								SocketStatus.CONNECTION_LOST);
					}
				} catch (IOException e) {
					logger.error("Unable to close connection!");
					throw ioe;
				}
			}
			throw ioe;
		}

	}

	@Override
	public synchronized void disconnect() {
		logger.info("try to close connection ...");

		try {
			tearDownConnection();
			for(ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}


	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			clientSocket.disconnect();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		String msg = "put"+" "+key+" "+value;
		if (value.equals("null")){
			value=null;
		}
		KVMSG newmsg=new KVMSG(key,value,StatusType.PUT);
		
		
		clientSocket.sendMessage(newmsg);
		logger.info("Send message:\t '" + msg+ "'");	

		KVMessage latestMsg = clientSocket.recieveKVMesssage();
		for(ClientSocketListener listener : listeners) {
			listener.handleNewPostKVMessage(latestMsg);
		}
		return latestMsg;

	}

	@Override
	public KVMessage get(String key) throws Exception {
		String msg = "get"+" "+key;

		KVMSG newmsg=new KVMSG(key,StatusType.GET);

		clientSocket.sendMessage(newmsg);

		logger.info("Send message:\t '" + msg+ "'");	
		KVMessage latestMsg = clientSocket.recieveKVMesssage();
		for(ClientSocketListener listener : listeners) {
			listener.handleNewGetKVMessage(latestMsg);
		}
		return latestMsg;
	}

	public void setRunning(boolean run) {
		running = run;
	}

	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}
	public boolean isRunning() {
		return running;
	}



}
