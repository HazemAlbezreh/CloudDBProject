package socket;

import java.io.*;
import java.net.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import ui.Application;


public class SocketClient {
	
	
	private Socket socket = null;

	private final static int carrigeReturn = 13;

	private Logger logger = Logger.getLogger(SocketClient.class);;
	
public SocketClient() {
	PropertyConfigurator.configure("logs/log.config");
	logger.setLevel(Application.currentLevel);
	}

/**
 * Connect to socket
 * @param host: The host to connect to
 * @param port: The port that the host use
 * @return the connection if successful
 */
	
	
	public boolean connect(String host, int port) {
		logger.setLevel(Application.currentLevel);
		try {
			this.socket = new Socket(host, port);
			
			
		} catch (ConnectException ConnectEx) {
			logger.debug("Cannot connect to Server, No response");
		} catch (IOException ioEx) {
			ioEx.printStackTrace();
		}
		return this.socket.isConnected();
	}
	/**
	 * Receive response from server after command execution
	 * @return the response from server
	 */
	public String recieve() {
		logger.setLevel(Application.currentLevel);
		if (this.socket != null) {
			return parseInput(this.socket);
		} else {
			return "You're not connected";
		}
	}
	
	/**
	 * Read the response from Echo server until it finishes
	 * @param socket server socket
	 * @return the server response
	 */
	private String parseInput(Socket socket) {
		logger.setLevel(Application.currentLevel);
		try {
			int AsciiVal = 0;
			String response = new String("");
			InputStream input = socket.getInputStream();
			do {
				AsciiVal = input.read();
				if ((AsciiVal != -1) && (AsciiVal != carrigeReturn)) {
					char c = (char) AsciiVal;
					response += c;
				}
			} while (AsciiVal != 13);
			return response;
		} catch (SocketTimeoutException TimeOutEx) {
			logger.debug("No response from server");
			return null;
		} catch (IOException ioEx) {
			ioEx.printStackTrace();
			return null;
		}
	}
	
	/**
	 * send a message to the echo server
	 * @param msg the message to send
	 * @return the status of the send operation
	 */
	public boolean send(String msg) {
		logger.setLevel(Application.currentLevel);
		if (this.socket != null) {
			try {
				OutputStream out = this.socket.getOutputStream();
				if (!(msg.charAt(msg.length() - 1) == (char) carrigeReturn)) {
					msg += (char) carrigeReturn;
				}
				out.write(msg.getBytes());
				out.flush();
				return true;
			} catch (SocketTimeoutException TimeOutEx) {
				logger.debug("No response from server");
				return false;
			} catch (IOException ioEx) {
				ioEx.printStackTrace();
				return false;
			}
		} else {
			return false;
		}
	}

	/**
	 * Disconnect from the echo server
	 * @return The status of the disconnect operation
	 */
	public boolean disconnect() {
		
	   if (socket != null) {
			try {
				
				socket.close(); // Step 4.
				socket=null;
				return true;
			} catch (IOException ioEx) {
				
				return false;
			}		
		}else {
			return false;
		}
	}
	
	/**
	 * Check if he connection is still available to the server
	 * @return True if the server is still connected, false otherwise
	 */
	public boolean isConnected() {
	
		try {
			return socket.isConnected();
		} catch (Exception e) {
			
			
			return false;
			
		}
	}	

	
	

}
