package app_kvServer;


import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.log4j.*;

import common.messages.*;
import common.messages.KVMessage.StatusType;
import config.ServerInfo;
import consistent_hashing.CommonFunctions;
import consistent_hashing.ConsistentHash;
import consistent_hashing.HashFunction;
import consistent_hashing.Md5HashFunction;
import consistent_hashing.Range;
import socket.SocketWrapper;
import ecs.EcsStore;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	//private static Logger logger = Logger.getRootLogger();
	private KVServer server;
	private SocketWrapper clientSocket;
	private boolean running;
	private HashFunction hashFunction;


	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket,KVServer server) {
		this.clientSocket = new SocketWrapper(clientSocket);
		this.server = server;
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
				if(message == null){
					if(this.clientSocket.isAlive()){
						continue;				//IF MESSAGE PARSE FAILS THEN WAIT NEXT MESSAGE
					}
					else{
						throw new IOException("ClientSocket closed ");
					}
				}
				switch (message.getMessageType()){
				case KVMESSAGE:
					if(server.getStatus()==ServerStatus.STOPPED || server.getStatus()==ServerStatus.INIT ){
						ClientMessage reply=new ClientMessage(KVMessage.StatusType.SERVER_STOPPED);
						clientSocket.sendMessage(reply);
						break;
					}
					
					ClientMessage cm = (ClientMessage)message;
					ClientMessage reply=null;
					String value=null,key=null;
					KVMessage.StatusType replyStatus=null;
					String searchSet=null;
					
					switch(cm.getStatus()){
					case GET:
						key = cm.getKey();
						if( !this.server.inRange(this.hashFunction.hash(key)) && !this.server.inReplicaRange(this.hashFunction.hash(key)) ){		//NOT IN RANGE
								reply= new ClientMessage(this.server.getMetadata());
								this.clientSocket.sendMessage(reply);
								break;
						}
						
						if( this.server.inRange(this.hashFunction.hash(key)) ){						//DECIDE WHAT DATASET TO SEARCH
							searchSet=this.server.getKVCache().getDatasetName();
						}else if( this.server.inReplicaRange(this.hashFunction.hash(key)) ){
							searchSet=this.server.getKVCache().getReplicaName();
						}
						
						value = this.server.getKVCache().processGetRequest(cm.getKey(),searchSet);
						if(value == null){										
							replyStatus=KVMessage.StatusType.GET_ERROR;
						}else{
							replyStatus=KVMessage.StatusType.GET_SUCCESS;
						//	logger.info("Get is successful " + cm.getKey() +" "+value);
						}
						reply= new ClientMessage(cm.getKey(),value,replyStatus);
						clientSocket.sendMessage(reply);
						break;		
					case PUT:
						key = cm.getKey();
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
								replyStatus=KVMessage.StatusType.valueOf(this.server.getKVCache().deleteDatasetEntry(l,this.server.getKVCache().getDatasetName()));
								if(replyStatus==StatusType.DELETE_ERROR){
								//	logger.info("Delete is not successful");
									reply=new ClientMessage(replyStatus);
									clientSocket.sendMessage(reply);
								}else{
								//	logger.info("Delete is successful");
									this.server.addToTimer(key, value);			//ADD TO MESSAGE QUEUE FOR REPLICAS
									
									reply=new ClientMessage(key,replyStatus);
									clientSocket.sendMessage(reply);
								}
							}else{
								replyStatus=KVMessage.StatusType.valueOf(this.server.getKVCache().processPutRequest(key, value, this.server.getKVCache().getDatasetName()));
								if(replyStatus==StatusType.PUT_SUCCESS){ 		//PUT SUCCESS
								//	logger.info("Put is successful");
									
									this.server.addToTimer(key, value);			//ADD TO MESSAGE QUEUE FOR REPLICAS
									
									reply=new ClientMessage(key,value,replyStatus);
									clientSocket.sendMessage(reply);
								}else if(replyStatus==StatusType.PUT_ERROR){	//PUT ERROR
								//	logger.info("Put is not successful");
									reply=new ClientMessage(key,value,replyStatus);
									clientSocket.sendMessage(reply);
								}else{											//PUT UPDATE
								//	logger.info("Put update is successful");
									
									this.server.addToTimer(key, value);			//ADD TO MESSAGE QUEUE FOR REPLICAS
									
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
						String caseType = sm.getMoveCase();
						String result2 = "PUT_SUCCESS";
						ArrayList<String> keys= new ArrayList<String>();
						String result=this.server.getKVCache().processMassPutRequest(h,this.server.getKVCache().getDatasetName());
						
						if(caseType.equals(ECSMessage.MoveCaseType.DELETE_NODE.toString())){
							for(String keytemp :h.keySet()){				
								keys.add(keytemp);
							}
							result2 = this.server.getKVCache().deleteDatasetEntry(keys, server.getKVCache().getReplicaName()); //TODO ERROR PROBABLY HERE
						}
						if(result.equals("PUT_ERROR") || result2.equals("DELETE_ERROR")){
							serverReply=new ServerMessage(ServerMessage.StatusType.DATA_TRANSFER_FAILED);
							this.clientSocket.sendMessage(serverReply);
				//			logger.error("Mass put ended with PUT_ERROR");

						}else{
							serverReply=new ServerMessage(ServerMessage.StatusType.DATA_TRANSFER_SUCCESS);
							this.clientSocket.sendMessage(serverReply);
				//			logger.info("Mass Put is successful");
						}
						break;
					case DELETEFROM_REPLICA:  
						Range rng = sm.getRange();
						if(rng == null){
							Map<String,String> data =sm.getData();
							 keys= new ArrayList<String>();
							for(String keytemp :data.keySet()){				
								keys.add(keytemp);
							}
							String delresult = this.server.getKVCache().deleteDatasetEntry(keys,this.server.getKVCache().getReplicaName());
							if(delresult.equals("DELETE_ERROR")){
								serverReply=new ServerMessage(ServerMessage.StatusType.REPLICA_FAILURE);
								this.clientSocket.sendMessage(serverReply);
								}
							else{
								serverReply=new ServerMessage(ServerMessage.StatusType.DELETEFROM_REPLICA_SUCCESS);//correct the status
								this.clientSocket.sendMessage(serverReply);
					//			logger.info("Mass Put is successful");
							}
						}
						else{
								Map<String,String>mp =  this.server.getKVCache().findValuesInRange(rng, hashFunction, this.server.getKVCache().getReplicaName());
							    keys= new ArrayList<String>();
								for(String keytemp : mp.keySet()){				
									keys.add(keytemp);
								}
								String res = this.server.getKVCache().deleteDatasetEntry(keys, this.server.getKVCache().getReplicaName());
								if(res.equals("DELETE_ERROR")){
									serverReply=new ServerMessage(ServerMessage.StatusType.DELETEFROM_REPLICA_FAILED); //correct statuses 
									this.clientSocket.sendMessage(serverReply);
									throw new Exception("Delete speceific range of data from serve failed!");
								}
								else{
									serverReply=new ServerMessage(ServerMessage.StatusType.DELETEFROM_REPLICA_SUCCESS); //correct statuses 
									this.clientSocket.sendMessage(serverReply);
								}
						}
						
						break;
					case INIT_REPLICA:
						Map<String,String> dta =sm.getData();
						String initResult=this.server.getKVCache().processMassPutRequest(dta,this.server.getKVCache().getReplicaName());
						if(initResult.equals("PUT_ERROR")){
							serverReply=new ServerMessage(ServerMessage.StatusType.REPLICA_FAILURE);
							this.clientSocket.sendMessage(serverReply);

						}
						else{
							serverReply=new ServerMessage(ServerMessage.StatusType.DATA_TRANSFER_SUCCESS); //correct the status
							this.clientSocket.sendMessage(serverReply);
						}
						
						break;
						
					case UPDATE_REPLICA:	//TODO
						Map<String,String> upData =sm.getData();
						for(Map.Entry<String, String> entry : upData.entrySet()){
							this.server.getKVCache().processPutRequest((String)entry.getKey(), (String)entry.getValue(), this.server.getKVCache().getDatasetName());
						}
						break;
						
					default :
						serverReply=new ServerMessage(ServerMessage.StatusType.REPLICA_FAILURE);
						clientSocket.sendMessage(serverReply);
						break;
					}
					break;
					
					
				case CONFIGMESSAGE:
					ECSMessage config = (ECSMessage)message;
					ECSMessage ecsReply=null;
					boolean result=false;
					
					switch(config.getStatus()){
					case INIT:
						result=this.server.initKVServer(config.getCacheSize(), config.getCacheStrategy(), config.getRing(),config.getRange(),config.getReplicaRange());
						if(result){
							ecsReply=new ECSMessage(ConfigMessage.StatusType.INIT_SUCCESS);
						}else{
							ecsReply=new ECSMessage(ConfigMessage.StatusType.INIT_FAILURE);
						}
						clientSocket.sendMessage(ecsReply);
						break;
					case START:
						result=this.server.startServer();
						if(result){
							ecsReply=new ECSMessage(ConfigMessage.StatusType.START_SUCCESS);
						}else{
							ecsReply=new ECSMessage(ConfigMessage.StatusType.START_FAILURE);
						}
						clientSocket.sendMessage(ecsReply);
						break;
					case STOP:
						result=this.server.stopServer();
						if(result){
							ecsReply=new ECSMessage(ConfigMessage.StatusType.STOP_SUCCESS);
						}else{
							ecsReply=new ECSMessage(ConfigMessage.StatusType.STOP_FAILURE);
						}
						clientSocket.sendMessage(ecsReply);
						break;
					case LOCK_WRITE:	
						result=this.server.lockWrite();
						if(result){
							ecsReply=new ECSMessage(ConfigMessage.StatusType.LOCK_WRITE_SUCCESS);
						}else{
							ecsReply=new ECSMessage(ConfigMessage.StatusType.LOCK_WRITE_FAILURE);
						}
						clientSocket.sendMessage(ecsReply);
						break;
					case UN_LOCK_WRITE:
						result=this.server.unlockWrite();
						if(result){
							ecsReply=new ECSMessage(ConfigMessage.StatusType.UN_LOCK_WRITE_SUCCESS);
						}else{
							ecsReply=new ECSMessage(ConfigMessage.StatusType.UN_LOCK_WRITE_FAILURE);
						}
			//			logger.info("server status : " + this.server.getStatus());
						clientSocket.sendMessage(ecsReply);
				//		logger.info("message sent : " + ecsReply.getStatus());
						break;
					case HEART_BEAT:
						ecsReply=new ECSMessage(ConfigMessage.StatusType.HEART_BEAT_ALIVE);
						clientSocket.sendMessage(ecsReply);
						break;
						
					case UPDATE_META_DATA:
						SortedMap<Integer, ServerInfo> oldRing = this.server.getMetadata();
						int serverKey = this.server.getRange().getHigh();
						this.server.update(config.getRing(), config.getRange(),config.getReplicaRange());
						ecsReply=new ECSMessage(ConfigMessage.StatusType.UPDATE_META_DATA_SUCCESS);
						clientSocket.sendMessage(ecsReply);
						
						ServerInfo currentServer = this.server.getMetadata().get(serverKey);
						
						ServerInfo oldSuccessor = CommonFunctions.getSuccessorNode(currentServer, oldRing);
						ServerInfo newSuccessor = CommonFunctions.getSuccessorNode(currentServer, this.server.getMetadata());
						ServerInfo oldSecondSuccessor = CommonFunctions.getSecondSuccessorNode(currentServer, oldRing);
						ServerInfo oldPredecessor = CommonFunctions.getPredecessorNode(currentServer, oldRing);
						ServerInfo newSecondSuccessor = CommonFunctions.getSecondSuccessorNode(currentServer, this.server.getMetadata());
						ServerInfo newPredecessor = CommonFunctions.getPredecessorNode(currentServer, this.server.getMetadata());
						
						Map<String,String> serverDatabase = null;
						ServerMessage coordinatormsg = null;
						EcsStore connection = null;
						SocketWrapper socket = null;
						ServerMessage response = null;

						if(this.server.getMetadata().size() > oldRing.size() && oldRing.size() >0){    //in this case we have (new added node case)
							if(!oldSuccessor.equals(newSuccessor)){//case number one
								serverDatabase = this.server.getKVCache().findValuesInRange(this.server.getRange(), this.hashFunction, this.server.getKVCache().getDatasetName());
								if( this.server.getMetadata().size() > 3){ //if we have 4 nodes and so on.
									try{
										coordinatormsg = new ServerMessage(ServerMessage.StatusType.DELETEFROM_REPLICA, serverDatabase);
										//connect to successor number three "it was before replica2" but on new ring it is not anymore and the data should be delete from this replica 
										connection = new EcsStore(oldSecondSuccessor.getServerIP(), oldSecondSuccessor.getPort());
										connection.connect();
										socket = connection.getSocketWrapper();
										socket.sendMessage(coordinatormsg);
										response = (ServerMessage)socket.recieveMesssage();
										if(response.getStatus()== ServerMessage.StatusType.REPLICA_FAILURE){
											socket.disconnect();
											throw new Exception("KVServer responded with " + response.getStatus().toString());
										}
										socket.disconnect();
										//connect to the new added node and hand over the data to it to be replica 1
										coordinatormsg = new ServerMessage(ServerMessage.StatusType.INIT_REPLICA, serverDatabase);
										connection = new EcsStore(newSuccessor.getServerIP(), newSuccessor.getPort());
										connection.connect();
										socket = connection.getSocketWrapper();
										socket.sendMessage(coordinatormsg);
										response = (ServerMessage)socket.recieveMesssage();
										socket.disconnect();
										if(response.getStatus()== ServerMessage.StatusType.REPLICA_FAILURE)
											throw new Exception("KVServer responded with " + response.getStatus().toString());
									}
									catch(IOException e){
										
									}
								}// if the ring size = 2 or 3
								else{
									//connect to the new added node and hand over the data to it to be replica 1
										try
										{
											coordinatormsg = new ServerMessage(ServerMessage.StatusType.INIT_REPLICA, serverDatabase);
											connection = new EcsStore(newSuccessor.getServerIP(), newSuccessor.getPort());
											connection.connect();
											socket = connection.getSocketWrapper();
											socket.sendMessage(coordinatormsg);
											response = (ServerMessage)socket.recieveMesssage();
											socket.disconnect();
											if(response.getStatus()== ServerMessage.StatusType.REPLICA_FAILURE)
												throw new Exception("KVServer responded with " + response.getStatus().toString());
										}
										catch(IOException e){
											
										}
								}
							}
							
							// case number 2. in this the new node is added to be the second successor of the current node
							else if(!oldSecondSuccessor.equals(newSecondSuccessor)){
								serverDatabase = this.server.getKVCache().findValuesInRange(this.server.getRange(), this.hashFunction, this.server.getKVCache().getDatasetName());
								if( this.server.getMetadata().size() > 3){  // we have more than three nodes.
									try{
										coordinatormsg = new ServerMessage(ServerMessage.StatusType.DELETEFROM_REPLICA, serverDatabase);
										//connect to successor number three "it was before replica2" but on new ring it is not anymore and the data should be delete from this replica 
										connection = new EcsStore(oldSecondSuccessor.getServerIP(), oldSecondSuccessor.getPort());
										connection.connect();
										socket = connection.getSocketWrapper();
										socket.sendMessage(coordinatormsg);
										response = (ServerMessage)socket.recieveMesssage();
										if(response.getStatus()== ServerMessage.StatusType.REPLICA_FAILURE){
											socket.disconnect();
											throw new Exception("KVServer responded with " + response.getStatus().toString());
										}
										socket.disconnect();
										//connect to the new added node and hand over the data to it to be replica 2
										coordinatormsg = new ServerMessage(ServerMessage.StatusType.INIT_REPLICA, serverDatabase);
										connection = new EcsStore(newSecondSuccessor.getServerIP(), newSecondSuccessor.getPort());
										connection.connect();
										socket = connection.getSocketWrapper();
										socket.sendMessage(coordinatormsg);
										response = (ServerMessage)socket.recieveMesssage();
										socket.disconnect();
										if(response.getStatus()== ServerMessage.StatusType.REPLICA_FAILURE)
											throw new Exception("KVServer responded with " + response.getStatus().toString());
									}
									catch(IOException e){
										
									}
								}  
								//in this case we have 3 nodes only
								else{
									//connect to the new added node and hand over the data to it to be replica 2
										try
										{
											coordinatormsg = new ServerMessage(ServerMessage.StatusType.INIT_REPLICA, serverDatabase);
											connection = new EcsStore(newSecondSuccessor.getServerIP(), newSecondSuccessor.getPort());
											connection.connect();
											socket = connection.getSocketWrapper();
											socket.sendMessage(coordinatormsg);
											response = (ServerMessage)socket.recieveMesssage();
											socket.disconnect();
											if(response.getStatus()== ServerMessage.StatusType.REPLICA_FAILURE)
												throw new Exception("KVServer responded with " + response.getStatus().toString());
										}
										catch(IOException e){
											
										}
								}
							}
							
							else if(!oldPredecessor.equals(newPredecessor)){
							//in move data case we will not delete the data from the node after handing it to other node but we will 
							//put the data out of range in the replica file of the node so that the node becomes replica 1 for the new node.
								try
								{
									if(this.server.getMetadata().size() >3){
										Range newRange = new Range(Md5HashFunction.getInstance().hash(oldPredecessor), this.server.getRange().getLow());
										coordinatormsg = new ServerMessage(ServerMessage.StatusType.INIT_REPLICA, newRange);
										connection = new EcsStore(newSecondSuccessor.getServerIP(), newSecondSuccessor.getPort());
										connection.connect();
										socket = connection.getSocketWrapper();
										socket.sendMessage(coordinatormsg);
										response = (ServerMessage)socket.recieveMesssage();
										socket.disconnect();
										if(response.getStatus()== ServerMessage.StatusType.REPLICA_FAILURE)
											throw new Exception("KVServer responded with " + response.getStatus().toString());
									}
								}

								catch(IOException e){
									
								}
							}
						}
						//in this case we have deleted node case
						else if(this.server.getMetadata().size() < oldRing.size() && this.server.getMetadata().size() >=3){
							//case 1 successor number 1,   //case 2 successor number 2
							if(!oldSuccessor.equals(newSuccessor) || !oldSecondSuccessor.equals(newSecondSuccessor)){
								try{
									serverDatabase = this.server.getKVCache().findValuesInRange(this.server.getRange(), this.hashFunction, this.server.getKVCache().getDatasetName());
									coordinatormsg = new ServerMessage(ServerMessage.StatusType.INIT_REPLICA, serverDatabase);
									connection = new EcsStore(newSecondSuccessor.getServerIP(), newSecondSuccessor.getPort());
									connection.connect();
									socket = connection.getSocketWrapper();
									socket.sendMessage(coordinatormsg);
									response = (ServerMessage)socket.recieveMesssage();
									socket.disconnect();
									if(response.getStatus()== ServerMessage.StatusType.REPLICA_FAILURE)
										throw new Exception("KVServer responded with " + response.getStatus().toString());	
								}
								catch(IOException e){
									
								}
							}
							
							//case 2 predecessor number 1 processing
							else if(!oldPredecessor.equals(newPredecessor)){
								//in move data case we will not delete the data from the node after handing it to other node but we will 
								//but the data out of range in the replica file so that the node becomes replica 1 for the new node
									try{	
										Range newRange = new Range(this.server.getRange().getLow(), Md5HashFunction.getInstance().hash(oldPredecessor));
										serverDatabase = this.server.getKVCache().findValuesInRange(newRange, this.hashFunction, this.server.getKVCache().getDatasetName());
										coordinatormsg = new ServerMessage(ServerMessage.StatusType.DATA_TRANSFER, serverDatabase);
										connection = new EcsStore(oldSecondSuccessor.getServerIP(), oldSecondSuccessor.getPort());
										connection.connect();
										socket = connection.getSocketWrapper();
										socket.sendMessage(coordinatormsg);
										response = (ServerMessage)socket.recieveMesssage();
										socket.disconnect();
										if(response.getStatus()== ServerMessage.StatusType.REPLICA_FAILURE)
											throw new Exception("KVServer responded with " + response.getStatus().toString());
									}
	
									catch(IOException e){
										
									}
								}
							}
						
						break;
					case MOVE_DATA:
						ServerInfo receipient = config.getServerInfo();
						Range dataRange=config.getRange();
						String caseType = config.getMoveDatacase().toString();
						Map<String,String> dataSet=this.server.getKVCache().findValuesInRange(dataRange,this.hashFunction,this.server.getKVCache().getDatasetName()); //CALCULATE RANGE TO TRANSFER
						ServerMessage dataMap=new ServerMessage(dataSet,config.getMoveDatacase());
						try{
							EcsStore ecsStore = new EcsStore(receipient.getServerIP(), receipient.getPort());
							ecsStore.connect();
							SocketWrapper target = ecsStore.getSocketWrapper();
							target.sendMessage(dataMap);
							ServerMessage recReply=(ServerMessage)target.recieveMesssage();
							if(recReply.getStatus()==ServerMessage.StatusType.DATA_TRANSFER_SUCCESS){	//IF SUCCESSFUL
								ecsReply=new ECSMessage(ConfigMessage.StatusType.MOVE_DATA_SUCCESS);	
								clientSocket.sendMessage(ecsReply);
								ArrayList<String> keys= new ArrayList<String>();						//CREATE A LIST OF THE KEYS IN THE SENT MAP
								for(String keytemp :dataSet.keySet()){				
									keys.add(keytemp);
								}
								//DELETE KEYS THAT WERE TRANSFERED
								this.server.getKVCache().deleteDatasetEntry(keys,this.server.getKVCache().getDatasetName());
								//if we are in case of delete node then we need also to free the replica of this deleted node
								if(caseType.equals(ECSMessage.MoveCaseType.DELETE_NODE.toString()))
									this.server.getKVCache().deleteAllData(this.server.getKVCache().getReplicaName());
								//Mahmoud -- add the data which is out of range to be a Replica1 of the node.
								if(caseType.equals(ECSMessage.MoveCaseType.ADD_NODE.toString()))
									this.server.getKVCache().processMassPutRequest(dataSet, this.server.getKVCache().getReplicaName());
									
						//		logger.info("Move data sent successfully ... Removing keys from cache");
							}else{
								throw new Exception("KVServer responded with " + recReply.getStatus().toString());
							}
							target.disconnect();
						}catch(Exception e){
							ecsReply=new ECSMessage(ConfigMessage.StatusType.MOVE_DATA_FAILURE);
							clientSocket.sendMessage(ecsReply);
						//	logger.error("Data transfer to KVServer failed with IOException "+e.getMessage());
						}
						break;
					case SHUT_DOWN:
						this.server.shutDown();
						break;
						
					case RECOVER_FAILD_NODE:
						Range absorbRange=config.getRange();
						Map<String,String> absorbData=this.server.getKVCache().findValuesInRange(absorbRange,this.hashFunction,this.server.getKVCache().getReplicaName()); 
						String insertResult=this.server.getKVCache().processMassPutRequest(absorbData,this.server.getKVCache().getDatasetName());
						
						ArrayList<String> keysAdded= new ArrayList<String>();						//CREATE A LIST OF THE KEYS IN THE SENT MAP
						for(String keytemp :absorbData.keySet()){				
							keysAdded.add(keytemp);
						}
						
						String deleteResult=this.server.getKVCache().deleteDatasetEntry(keysAdded, server.getKVCache().getReplicaName());
						
						if(insertResult.equals("PUT_ERROR") || deleteResult.equals("DELETE_ERROR")){
							ecsReply=new ECSMessage(ConfigMessage.StatusType.RECOVER_FAILD_NODE_FAILURE);
							this.clientSocket.sendMessage(ecsReply);
				//			logger.error("Recover put ended with PUT_ERROR");
						}else{
							ecsReply=new ECSMessage(ConfigMessage.StatusType.RECOVER_FAILD_NODE_SUCCESS);
							this.clientSocket.sendMessage(ecsReply);
				//			logger.info("Recover Put is successful");
						}
						break;
						
					default:
				//		logger.debug("Let's Hope this does not get printed or I forgot a message type");
						break;
					}
		
					break;
				default : 
			//		logger.debug("Let's Hope this does not get printed");
					break;
				}
			}
	//		logger.info("Thread Shutdown normally");
		}catch(Exception e){
	//		logger.error("Received Exception at ClientConnection "+e.getMessage());
			if(isRunning()){
	//			logger.error("Received Exception at ClientConnection While running "+e.getMessage());
				this.server.removeThread(this);
			}else{
	//			logger.info("Received Terminate Thread " + e.getMessage());
			}
			this.clientSocket.disconnect();
		}
		
	}
	
	public synchronized void terminateThread() throws IOException{
	//	logger.info("Initiating Terminate Thread ");
		this.running=false;
		this.clientSocket.disconnect();
		return;
	}
	
	private synchronized boolean isRunning(){
		return running;
	}
}
