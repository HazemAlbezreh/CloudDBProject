package app_kvClient;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

import socket.SocketWrapper;
import common.messages.ClientMessage;
import common.messages.Message;

public class Subscription extends Thread {
	private boolean running=true;
	private SocketWrapper clientSocket;
	private ServerSocket serverSocket;
	private Socket socket;
	private Logger logger = Logger.getRootLogger();

	public Subscription(ServerSocket serverSocket) throws IOException {
		this.serverSocket=serverSocket;
		this.running=true;

		
	}

	/**
	 * This thread waits for a connection to be accepted with a server. Once a connection being made, 
	 * it waits to receive a message and then analyses it. When finished, it closes the socket
	 */
	public void run() {
		String key,value;
		try{
			
			while(isRunning()){
				this.socket = serverSocket.accept();
				this.clientSocket = new SocketWrapper(socket);
				Message message=this.clientSocket.recieveMesssage();
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
					ClientMessage cm = (ClientMessage)message;
					switch(cm.getStatus()){
					case SUBSCRIBE_UPDATE:
						key = cm.getKey();
						if (cm.getValue()==null){
							System.out.println("Key= "+key+" has been deleted!! ");
							System.out.print("EchoClient>");
							logger.info("Received SUBSCRIBE_UPDATE - The subscribed key "+key+"has been deleted!");
						}else{
							value=cm.getValue();
							System.out.println("Key= "+key+" has been changed into new value of "+value+"!!");
							System.out.print("EchoClient>");
							logger.info("Received SUBSCRIBE_UPDATE - The subscribed key "+key+"has been changed to value "+value+"!");
						}
						socket.close();
						break;
					default:
						//IGNORE OTHER MESSAGES
						break;
					}
					break;
					
				default:
					//IGNORE OTHER MESSAGES
					break;
				}
			}
		}catch(Exception e){
			this.running=false;
			this.clientSocket.disconnect();
		}

	}


	private synchronized boolean isRunning(){
		return running;
	}

}
