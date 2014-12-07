package app_kvServer;


import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.*;

import common.messages.*;
import common.messages.KVMessage.StatusType;
import consistent_hashing.HashFunction;
import consistent_hashing.Md5HashFunction;
import socket.SocketWrapper;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection2 implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	private KVServer server;
	private SocketWrapper clientSocket;
	private KVCache cache;
	private boolean running;
	private HashFunction hashFunction;


	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection2(Socket clientSocket,KVServer server) {
		this.clientSocket = new SocketWrapper(clientSocket);
		this.server = server;
		this.cache=server.getKVCache();
		this.running=true;
		this.hashFunction=Md5HashFunction.getInstance();
	}

	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try{
			clientSocket.sendTextMessage(new TextMessage(
								"Connection to MSRG Echo server established: " 
										+ clientSocket.getSocket().getLocalAddress() + " / "
										+ clientSocket.getSocket().getLocalPort()));
			while(isRunning()){
				Message message=clientSocket.recieveMesssage();
				switch (message.getMessageType()){
				case KVMESSAGE:
					if(server.getStatus()==ServerStatus.STOPPED){
						ClientMessage reply=new ClientMessage(KVMessage.StatusType.SERVER_STOPPED);
						clientSocket.sendMessage(reply);
						break;
					}
					
					ClientMessage cm = (ClientMessage)message;
					ClientMessage reply=null;
					String value=null,key=null;
					KVMessage.StatusType replyStatus=null;
	
					
					switch(cm.getStatus()){
					case GET:
						key = cm.getKey();
						if(!this.server.inRange(this.hashFunction.hash(key))){		//NOT IN RANGE
							reply= new ClientMessage(this.server.getMetadata());
							this.clientSocket.sendMessage(reply);
						}
						value = cache.processGetRequest(cm.getKey());
						if(value == null){										
							replyStatus=KVMessage.StatusType.GET_ERROR;
						}else{
							replyStatus=KVMessage.StatusType.GET_SUCCESS;
							logger.info("Get is successful " + cm.getKey() +" "+value);
						}
						reply= new ClientMessage(cm.getKey(),value,replyStatus);
						clientSocket.sendMessage(reply);
						break;		
					case PUT:
						if(server.getStatus()==ServerStatus.LOCKED){					//WRITE LOCK
							reply=new ClientMessage(StatusType.SERVER_WRITE_LOCK);
							clientSocket.sendMessage(reply);
						}else if(!this.server.inRange(this.hashFunction.hash(key))){	// NOT IN RANGE
							reply= new ClientMessage(this.server.getMetadata());
							this.clientSocket.sendMessage(reply);
						}else{															//DOABLE
							value=cm.getValue();
							key=cm.getKey();
							if(value==null){		//delete key
								ArrayList<String> l=new ArrayList<String>();
								l.add(key);
								replyStatus=KVMessage.StatusType.valueOf(cache.deleteDatasetEntry(l));
								if(replyStatus==StatusType.DELETE_ERROR){
									logger.info("Delete is not successful");
									reply=new ClientMessage(replyStatus);
									clientSocket.sendMessage(reply);
								}else{
									logger.info("Delete is successful");
									reply=new ClientMessage(key,replyStatus);
									clientSocket.sendMessage(reply);
								}
							}else{
								HashMap<String,String> h=new HashMap<String,String>();
								h.put(key, value);
								replyStatus=KVMessage.StatusType.valueOf(cache.processPutRequest(h));
								if(replyStatus==StatusType.PUT_SUCCESS){ 		//PUT SUCCESS
									logger.info("Put is successful");
									reply=new ClientMessage(key,value,replyStatus);
									clientSocket.sendMessage(reply);
								}else if(replyStatus==StatusType.PUT_ERROR){	//PUT ERROR
									logger.info("Put is not successful");
									reply=new ClientMessage(key,value,replyStatus);
									clientSocket.sendMessage(reply);
								}else{											//PUT UPDATE
									logger.info("Put update is successful");
									reply=new ClientMessage(key,value,replyStatus);
									clientSocket.sendMessage(reply);
								}
							}
						}
						break;
					default:
						reply=new ClientMessage("error","Wrong KVMessage Status received",KVMessage.StatusType.FAILURE);
						clientSocket.sendMessage(reply);
						break;
					}
					break;
				case SERVERMESSAGE:
					ServerMessage sm = (ServerMessage) message;
					ServerMessage serverReply=null;
					
					switch(sm.getStatus()){
					case DATA_TRANSFER:
						Map<String,String> h =sm.getData();
						String result=cache.processPutRequest(h);
						if(result.equals("PUT_ERROR")){
							serverReply=new ServerMessage(ServerMessage.StatusType.DATA_TRANSFER_FAILED);
							this.clientSocket.sendMessage(serverReply);
							logger.error("Mass put ended with PUT_ERROR");

						}else{
							serverReply=new ServerMessage(ServerMessage.StatusType.DATA_TRANSFER_SUCCESS);
							this.clientSocket.sendMessage(serverReply);
							logger.info("Mass Put is successful");
						}
						break;
					default :
						serverReply=new ServerMessage(ServerMessage.StatusType.FAILURE);
						clientSocket.sendMessage(serverReply);
						break;
					}
					break;
				case CONFIGMESSAGE:
					ConfigMessage config = (ConfigMessage)message;
					
					break;
				default : 
					logger.debug("Let's Hope this does not get printed");
					break;
				}
			}
		}catch(Exception e){
			//TODO HANDLE EXCEPTIONS
		}
	}
	
	public void terminateThread() throws IOException{
		return;
	}
	
	private synchronized boolean isRunning(){
		return running;
	}
}