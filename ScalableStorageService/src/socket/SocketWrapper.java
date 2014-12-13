package socket;

import java.io.*;
import java.net.*;

import org.apache.log4j.Logger;

import common.messages.Message;
import common.messages.MessageFactory;
import common.messages.MessageParseException;
import common.messages.TextMessage;



public class SocketWrapper {

	private Socket socket = null;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
//	private Logger logger = Logger.getLogger(SocketWrapper.class);;

	public SocketWrapper() {

	}

	public SocketWrapper(Socket socket) {
		this.setSocket(socket);
	}

	/**
	 * Connect to socket
	 * 
	 * @param host
	 *            : The host to connect to
	 * @param port
	 *            : The port that the host use
	 * @return the connection if successful
	 * @throws IOException
	 */

	public boolean connect(String host, int port) throws IOException {

		try {
			this.setSocket(new Socket(host, port));

		} catch (ConnectException ConnectEx) {
			//logger.debug("Cannot connect to Server, No response");
		} catch (IOException ioEx) {
//			ioEx.printStackTrace();
			throw ioEx;
		}
		return this.getSocket().isConnected();
	}


	/**
	 * Disconnect from the echo server
	 * 
	 * @return The status of the disconnect operation
	 */
	public boolean disconnect() {

		if (getSocket() != null) {
			try {
				getSocket().close(); // Step 4.
			//	logger.info("Client Disconnected ");
				setSocket(null);
				return true;
			} catch (IOException ioEx) {

				return false;
			}
		} else {
			return false;
		}
	}

	/**
	 * Check if he connection is still available to the server
	 * 
	 * @return True if the server is still connected, false otherwise
	 */
	public boolean isConnected() {

		try {
			return getSocket().isConnected();
		} catch (Exception e) {

			return false;

		}
	}
	
	public boolean isAlive(){
		try {
			return (getSocket().isConnected() && !getSocket().isClosed());
		} catch (Exception e) {

			return false;

		}
	}
	

	public Socket getSocket() {
		return socket;
	}

	public void setSocket(Socket socket) {
		this.socket = socket;
	}
	
	
	
	
	
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendTextMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		OutputStream output = this.getSocket().getOutputStream();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
//		logger.info("SEND \t<" 
//				+ this.getSocket().getInetAddress().getHostAddress() + ":" 
//				+ this.getSocket().getPort() + ">: '" 
//				+ msg.getMsg() +"'");
    }
	
	public TextMessage receiveTextMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		InputStream input = this.socket.getInputStream();
		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read!=-1 && read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		if(read == -1 ){
			throw new IOException("End of stream");
		}
		
		/* build final String */
		TextMessage msg = new TextMessage(msgBytes);
//		logger.info("RECEIVE \t<" 
//				+ this.getSocket().getInetAddress().getHostAddress() + ":" 
//				+ this.getSocket().getPort() + ">: '" 
//				+ msg.getMsg().trim() + "'");
		return msg;
    }
	
		
	public boolean sendMessage(Message msg) {
		if (this.getSocket() != null) {
			try {
				TextMessage tm= new TextMessage(msg.getJson());
				this.sendTextMessage(tm);
				return true;
			} catch (SocketTimeoutException TimeOutEx) {
			//	logger.debug("No response from server");
				return false;
			} catch (IOException ioEx) {
				ioEx.printStackTrace();
			//	logger.debug("IOException sending message : "+ msg.getClass().getName()+" : "+ioEx.getMessage());
				disconnect();
				return false;
			}
		} else {
			return false;
		}
	}

	public Message recieveMesssage() {
		Message message = null;
		try {
			String s = this.receiveTextMessage().getMsg();
			message= MessageFactory.parse(s);			
		} catch (IOException e) {
//			e.printStackTrace();
			disconnect();
		} catch (MessageParseException e){
		//	logger.debug("Unknown Message Format received : "+e.getMessage());	
			message=null;
		}
		return message;
	}

}
