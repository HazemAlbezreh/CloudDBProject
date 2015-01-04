package ecs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import socket.SocketWrapper;
import client.ClientSocketListener;
import client.ClientSocketListener.SocketStatus;
import common.messages.ConfigMessage;
import common.messages.ECSMessage;
import common.messages.Message;
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
			TextMessage latestMsg = clientSocket.receiveTextMessage();
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
	public ECSMessage initServer(SortedMap<Integer, ServerInfo> ring,Range range,int cacheSize, String strategy){
		ECSMessage reply;
		//creating ECS message 
		ECSMessage msg =new ECSMessage(ConfigMessage.StatusType.INIT,range,ring,strategy,cacheSize);
		clientSocket.sendMessage(msg);
		logger.info("Send ECS message (update meta data):\t '" + msg+ "'");	
		Message latestMsg = clientSocket.recieveMesssage();
		reply=handleResponse(latestMsg);
		return reply;
	}
	
	@Override
	public ECSMessage updateMetaData(SortedMap<Integer, ServerInfo> ring,Range range) {
		
		ECSMessage reply;
		//creating ECS message 
		ECSMessage msg =new ECSMessage(ring,range);
		clientSocket.sendMessage(msg);
		logger.info("Send ECS message (update meta data):\t '" + msg+ "'");	
		Message latestMsg = clientSocket.recieveMesssage();
		reply=handleResponse(latestMsg);
		return reply;
	}




	@Override
	public ECSMessage start() {
		ECSMessage reply;
		//creating ECS message 
		ECSMessage msg =new ECSMessage(ConfigMessage.StatusType.START);
		clientSocket.sendMessage(msg);
		logger.info("Send ECS message (start):\t '" + msg + "'");	
		Message latestMsg = clientSocket.recieveMesssage();
		reply=handleResponse(latestMsg);
		return reply;
	}



	@Override
	public ECSMessage stop() {
		ECSMessage reply;
		//creating ECS message 
		ECSMessage msg =new ECSMessage(ConfigMessage.StatusType.STOP);
		clientSocket.sendMessage(msg);
		logger.info("Send ECS message (stop):\t '" + msg + "'");	
		Message latestMsg = clientSocket.recieveMesssage();
		reply=handleResponse(latestMsg);
		return reply;
	}



	@Override
	public ECSMessage lockWrite() {
		ECSMessage reply;
		//creating ECS message 
		ECSMessage msg =new ECSMessage(ConfigMessage.StatusType.LOCK_WRITE);
		clientSocket.sendMessage(msg);
		logger.info("Send ECS message (lock write):\t '" + msg + "'");	
		Message latestMsg = clientSocket.recieveMesssage();
		reply=handleResponse(latestMsg);
		return reply;
	}



	@Override
	public ECSMessage unLockWrite() {
		ECSMessage reply;
		//creating ECS message 
		ECSMessage msg =new ECSMessage(ConfigMessage.StatusType.UN_LOCK_WRITE);
		clientSocket.sendMessage(msg);
		logger.info("Send ECS message (unlock write):\t '" + msg + "'");	
		Message latestMsg = clientSocket.recieveMesssage();
		reply=handleResponse(latestMsg);
		return reply;
	}



	@Override
	public ECSMessage moveData(Range range, ServerInfo targetServer) {
		ECSMessage reply;
		//creating ECS message 
		ECSMessage msg =new ECSMessage(ConfigMessage.StatusType.MOVE_DATA,targetServer,range);
		clientSocket.sendMessage(msg);
		logger.info("Send ECS message (move data):\t '" + msg + "'");	
		Message latestMsg = clientSocket.recieveMesssage();
		reply=handleResponse(latestMsg);
		return reply;
	}



	@Override
	public void shutDown() {
		//creating ECS message 
		ECSMessage msg =new ECSMessage(ConfigMessage.StatusType.SHUT_DOWN);
		clientSocket.sendMessage(msg);
		logger.info("Send ECS message (unlock write):\t '" + msg + "'");
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

	public SocketWrapper getSocketWrapper(){
		return this.clientSocket;
	}
	
	
public ECSMessage handleResponse(Message latestMsg) {
		
	ECSMessage reply=null;
		switch (latestMsg.getMessageType()){
		case CONFIGMESSAGE:
			reply=(ECSMessage)latestMsg;
			break;
		default:
			logger.debug("Invalid Message type received" + latestMsg.getJson());	
			break;
		}
		return reply;
	}
	
}