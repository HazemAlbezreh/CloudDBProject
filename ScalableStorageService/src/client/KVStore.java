package client;


import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;


import socket.SocketWrapper;


import client.ClientSocketListener.SocketStatus;


import common.messages.ClientMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.Message;
import common.messages.TextMessage;
import config.ServerInfo;
import consistent_hashing.CommonFunctions;

public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	//private Set<ClientSocketListener> listeners;
	private boolean running;

	private SocketWrapper clientSocket;

	private String address;
	private int port;

	private SortedMap<Integer, ServerInfo> ring = null;


	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */

	public KVStore(String address, int port)  {
		this.address=address;
		this.port=port;
		//listeners = new HashSet<ClientSocketListener>();
		logger.info("Connection established");

	}
	
	
	public void handleTextMessage(TextMessage msg) {
			System.out.println(msg.getMsg());
		
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
			handleTextMessage(latestMsg);
		} catch (IOException ioe) {
			//if there was an error connecting
			if(isRunning()) {
				logger.error("Connection lost!");
				try {
					tearDownConnection();
				//	for(ClientSocketListener listener : listeners) {
						handleStatus(SocketStatus.CONNECTION_LOST);
				//	}
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
			handleStatus(SocketStatus.DISCONNECTED);
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
		ClientMessage reply;
		String msg = "put"+" "+key+" "+value;
		if (value.equals("null")){
			value=null;
		}
		//creating KVMSG message 
		ClientMessage newmsg=new ClientMessage(key,value,StatusType.PUT);


		if (ring==null){
			clientSocket.sendMessage(newmsg);
			logger.info("Send message:\t '" + msg+ "'");	

			Message latestMsg = clientSocket.recieveMesssage();
			reply=handleReceivedPutMsg(latestMsg,key,value);
			return reply;
		}
		else{
			
			
			ServerInfo si=CommonFunctions.getSuccessorNode(key,ring);
			if (si.getServerIP()!=this.address || si.getPort()!=this.port){
				//disconnects from the running server
				disconnect();
				//and connects to the new one
				this.address=si.getServerIP();
				this.port= si.getPort();
				connect();
			}
			clientSocket.sendMessage(newmsg);
			logger.info("Send message:\t '" + msg+ "'");	

			Message latestMsg = clientSocket.recieveMesssage();
			reply=handleReceivedPutMsg(latestMsg,key,value);
			return reply;

		}


	}
	
	
	public ClientMessage handleReceivedPutMsg(Message latestMsg,String key,String value) throws Exception{
		ClientMessage reply=null;
		switch (latestMsg.getMessageType()){
		case KVMESSAGE:
			reply=(ClientMessage)latestMsg;
			switch (reply.getStatus()) {
			case SERVER_NOT_RESPONSIBLE:
				updateMetaData(reply.getMetadata());
				put(key,value);
				break;
			case SERVER_STOPPED:
				System.out.println("The Server has stopped for some time!Please wait or try later!");
				break;
			case SERVER_WRITE_LOCK:
				System.out.println("The Server is busy! Please try put requests later!");
				break;
			case PUT_ERROR:
				System.out.println("Your request was not successful!");				
				break;
			case PUT_SUCCESS:
				System.out.println(reply.getStatus().name()+"  < "+reply.getKey()+","+reply.getValue()+" >");
				break;
			case PUT_UPDATE:
				System.out.println(reply.getStatus().name()+"  < "+reply.getKey()+","+reply.getValue()+" >");
				break;
			case DELETE_SUCCESS:
				System.out.println(reply.getStatus().name()+"  < "+reply.getKey());
				break;
			case DELETE_ERROR:
				System.out.println(reply.getStatus().name()+"  < "+reply.getKey());
				break;
			default:
				logger.debug("Invalid Message Status received" + latestMsg.getJson());	
			}
			break;
		default:
			logger.debug("Invalid Message type received" + latestMsg.getJson());	
			break;
		}
		return reply;

	}

	@Override
	public KVMessage get(String key) throws Exception {
		ClientMessage reply;
		String msg = "get"+" "+key;
		ClientMessage newmsg=new ClientMessage(key,StatusType.GET);

		if (ring==null){
			
			clientSocket.sendMessage(newmsg);

			logger.info("Send message:\t '" + msg+ "'");
			//wait until receive an answer 
			Message latestMsg = clientSocket.recieveMesssage();
			reply=handleReceivedGetMsg(latestMsg,key);
			return reply;

			
		}
		else{
			ServerInfo si=CommonFunctions.getSuccessorNode(key,ring);
			if (si.getServerIP()!=this.address || si.getPort()!=this.port){
				//disconnects from the running server
				disconnect();
				//and connects to the new one
				this.address=si.getServerIP();
				this.port= si.getPort();
				connect();
			}
			clientSocket.sendMessage(newmsg);
			logger.info("Send message:\t '" + msg+ "'");
			//wait until receive an answer 
			Message latestMsg = clientSocket.recieveMesssage();
			
			reply=handleReceivedGetMsg(latestMsg,key);
			return reply;
		}
		//sending the message to the socket
	}

	
	public ClientMessage handleReceivedGetMsg(Message latestMsg,String key) throws Exception{
		
		ClientMessage reply=null;
		switch (latestMsg.getMessageType()){
		case KVMESSAGE:
			reply=(ClientMessage)latestMsg;
			switch (reply.getStatus()) {
			case SERVER_NOT_RESPONSIBLE:
				updateMetaData(reply.getMetadata());
				get(key);
				break;
			case SERVER_STOPPED:
				System.out.println("The Server has stopped for some time!Please wait or try later!");
				break;
			case GET_ERROR:
				System.out.println("Tuple not found!");				
				break;
			case GET_SUCCESS:
				System.out.println(reply.getStatus()+"  < "+reply.getKey()+","+reply.getValue()+" >");
				break;
			default:
				logger.debug("Invalid Message Status received" + latestMsg.getJson());	
			}
			break;
		default:
			logger.debug("Invalid Message type received" + latestMsg.getJson());	
			break;
		}
		return reply;
	}
	
	
	public void setRunning(boolean run) {
		running = run;
	}
	
	
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.println("EchoClient> Connection terminated: " 
					+ address + " / " + port);

		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("EchoClient> Connection lost: " 
					+ address + " / " + port);
			System.out.print("EchoClient>");
		}

	}
	
	public boolean isRunning() {
		return running;
	}



	@Override
	public void updateMetaData(SortedMap<Integer, ServerInfo> updatedRing) {
		this.ring=updatedRing;
	}



	@Override
	public void shutDown() {
		// TODO Auto-generated method stub
		
	}



	

}
