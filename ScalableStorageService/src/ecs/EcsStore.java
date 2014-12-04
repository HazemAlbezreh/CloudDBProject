package ecs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import socket.SocketWrapper;
import client.ClientSocketListener;
import client.ClientSocketListener.SocketStatus;
import common.messages.ECSMessage;
import common.messages.TextMessage;
import config.ServerInfo;
import consistent_hashing.Range;

public class EcsStore implements EcsCommInterface {
	
	
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;

	private SocketWrapper clientSocket;

	private String address;
	private int port;
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */

	public EcsStore(String address, int port)  {
		this.address=address;
		this.port=port;
		listeners = new HashSet<ClientSocketListener>();
		logger.info("Connection established");
		
	}



	@Override
	public void connect() throws IOException {
		try {
			//connects to the socket of server
			clientSocket = new SocketWrapper();
			clientSocket.connect(this.address, this.port);
			setRunning(true);
			
			//waits until it receives the answer from server
			TextMessage latestMsg = clientSocket.receiveMessage();
			for(ClientSocketListener listener : listeners) {
				listener.handleNewMessage(latestMsg);
			}
		} catch (IOException ioe) {
			//if there was an error connecting
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
	public ECSMessage updateMetaData(SortedMap<Integer, ServerInfo> ring) {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public ECSMessage start() {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public ECSMessage stop() {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public ECSMessage lockWrite() {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public ECSMessage unLockWrite() {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public ECSMessage moveData(Range range, ServerInfo targetServer) {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public void shutDown() {
		// TODO Auto-generated method stub
		
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
