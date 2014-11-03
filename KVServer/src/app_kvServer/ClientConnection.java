package app_kvServer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.*;

import common.messages.KVMSG;
import common.messages.KVMessage.StatusType;
import common.messages.TextMessage;

import socket.SocketWrapper;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;

	private KVServer server;

	boolean firstMessage = true;
	private SocketWrapper clientSocket;


	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket,KVServer server) {
		this.clientSocket = new SocketWrapper(clientSocket);
		this.server = server;
		this.isOpen = true;
	}

	/**tr
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {

		StatusType statusType = null;
		String value=null;
		while (this.clientSocket.isConnected()) {
			if (firstMessage) {
				try {
					clientSocket.sendMessage(new TextMessage(
							"Connection to MSRG Echo server established: " 
									+ clientSocket.getSocket().getLocalAddress() + " / "
									+ clientSocket.getSocket().getLocalPort()));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				firstMessage = false;
			} else {
//				Set s = server.getKVCache().getCache().entrySet();
//				Iterator itr = s.iterator();
//				while(itr.hasNext()){
//					Map.Entry ent = (Map.Entry)itr.next();
//					System.out.println((String)ent.getKey() +" "+((MapValue)ent.getValue()).getValue()+" "+((MapValue)ent.getValue()).getCounter());				
//				}

				KVMSG latestMsg = clientSocket.recieveKVMesssage();
				if (latestMsg != null) {

					switch (latestMsg.getStatus()) {
					case GET:
						try {
							value = server.getKVCache().processGetRequest(latestMsg.getKey());
							logger.info("Get is successful");
							//							KVMSG kvmsg = new KVMSG(latestMsg.getKey(), value,
							//									StatusType.GET_SUCCESS);
							statusType = StatusType.GET_SUCCESS;
							//							clientSocket.sendMessage(kvmsg);
							if(value == null){
								statusType=statusType.GET_ERROR;
							}
						} catch (Exception e) {

							logger.error("Error! "
									+ "Error in get from store. \n", e);
							KVMSG kvmsg = new KVMSG(latestMsg.getKey(),
									StatusType.GET_ERROR);
							//							clientSocket.sendMessage(kvmsg);
							e.printStackTrace();
						}
						break;
					case PUT:
						try {
							value=latestMsg.getValue();
							if (latestMsg.getValue()==null ) {
								String status=server.getKVCache().deleteDatasetEntry(latestMsg.getKey());
								if(status.equals("DELETE_ERROR")){
									statusType=StatusType.DELETE_ERROR;
									logger.info("Delete is not successful");
								}
								else{
									logger.info("Delete is successful");
									statusType = StatusType.DELETE_SUCCESS;
								}
							} else {
								String status = server.getKVCache().processPutRequest(
										latestMsg.getKey(),
										latestMsg.getValue());
								if (status.equals(("PUT_SUCCESS"))){
									logger.info("Put is successful");
									statusType = StatusType.PUT_SUCCESS;
								}
								else if (status.equals(("PUT_ERROR"))){
									statusType = StatusType.PUT_ERROR;
									logger.info("Put is not successful");
								}
								else{
									statusType = StatusType.PUT_UPDATE;
									logger.info("Put update is successful");
								}

								//								statusType = server.getKVCache().processPutRequest(
								//										latestMsg.getKey(),
								//										latestMsg.getValue());
								//								logger.info("Put is successful");
							}
						} catch (Exception e) {
							//TODO Exception class for insertion error
							logger.error("Error! "
									+ "Error in put into store. \n", e);
							statusType = StatusType.PUT_ERROR;
							e.printStackTrace();
						}
						//						} catch (DeleteException e) {
						//							//TODO Exception class for insertion error
						//							logger.error("Error! "
						//									+ "Error in delete from store. \n", e);
						//							statusType = StatusType.DELETE_ERROR;
						//							e.printStackTrace();
						//						}
						break;

					default:

						break;
					}
					KVMSG kvmsg = new KVMSG(latestMsg.getKey(),value, statusType);
					clientSocket.sendMessage(kvmsg);
				}
			}
		}
	}






}