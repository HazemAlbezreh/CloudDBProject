package app_kvServer;


import java.io.IOException;
import java.net.Socket;
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
	}

	/**tr
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {

		StatusType statusType = null;
		String value=null;
		while (this.clientSocket.isAlive()) {
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

				KVMSG latestMsg = clientSocket.recieveKVMesssage();
				if (latestMsg != null) {

					switch (latestMsg.getStatus()) {
					case GET:
						try {
							value = server.getKVCache().processGetRequest(latestMsg.getKey());
							logger.info("Get is successful");
							statusType = StatusType.GET_SUCCESS;
							if(value == null){
								statusType=statusType.GET_ERROR;
							}
						} catch (Exception e) {

							logger.error("Error! "
									+ "Error in get from store. \n", e);
							statusType=statusType.GET_ERROR;
							e.printStackTrace();
						}
						break;
					case PUT:
						try {
							value=latestMsg.getValue();
					/*		if (latestMsg.getValue()==null ) {
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

							}
						*/
							}
						
						 catch (Exception e) {
							logger.error("Error! "
									+ "Error in put into store. \n", e);
							statusType = StatusType.PUT_ERROR;
						}
						break;

					default:
							statusType=StatusType.FAILURE;
							value="Invalid protocol requested ";
						break;
					}
					KVMSG kvmsg = new KVMSG(latestMsg.getKey(),value, statusType);
					clientSocket.sendMessage(kvmsg);
				}
				else{
					statusType=StatusType.FAILURE;
					value="Invalid Message format ";
					KVMSG kvmsg = new KVMSG("ERROR",value, statusType);
					clientSocket.sendMessage(kvmsg);
				}
			}
		}
		logger.info("Thread Terminated ");
	}
}