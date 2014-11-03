package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.TextMessage;


import client.ClientSocketListener;
import client.KVStore;
import client.ClientSocketListener.SocketStatus;





public class KVClient implements ClientSocketListener {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "EchoClient> ";
	private BufferedReader stdin;
	private boolean stop = false;

	private String serverAddress;
	private int serverPort;
	private KVStore client = null;



	public void kvClientrun() throws Exception {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);

			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

	private void handleCommand(String cmdLine) throws Exception {
		String[] tokens = cmdLine.split("\\s+");
		String key;
		String value;

		if(tokens[0].equals("quit")) {	
			stop = true;
			disconnect();
			System.out.println(PROMPT + "Application exit!");

		} else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					connect(serverAddress, serverPort);
				} catch(NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (IOException e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				}
			} else {
				printError("Invalid number of parameters!");
			}

		}  else if(tokens[0].equals("disconnect")) {
			disconnect();

		} else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}

		} 
		else if(tokens[0].equals("put")) {
			if(tokens.length >=2){
				key =tokens[1];
				if (tokens.length==3)
					value=tokens[2];
				else
					value=null;
				try{
					putTuples(key,value);
				} catch (IOException e) {
					printError("Could not execute put command!");
					logger.warn("Could not execute put command!", e);
				}
			}
			else {
				printError("Invalid number of parameters!");
			}
		}
		else if(tokens[0].equals("get")) {
			if(tokens.length ==2){
				key=tokens[1];
				try{
					getTuples(key);
				} catch (IOException e) {
					printError("Could not execute get command!");
					logger.warn("Could not execute get command!", e);
				}
			}
			else{
				printError("Invalid number of parameters!");

			}
		}
		else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
	}

	private void getTuples(String key) throws Exception {
		logger.info("Going to fetch value of key "+key);
		 client.get(key);

	}

	private void putTuples(String key, String value) throws Exception {
		logger.info("Going to put this pair of tuples < "+key+" , "+value+" >");
		client.put(key, value);
		
	}

	private void connect(String address, int port) 
			throws Exception {
		client = new KVStore(address, port);
		client.addListener(this);
		client.connect();
	}

	private void disconnect() {
		if(client != null) {
			client.disconnect();
			client = null;
		}
	}

	/**
	 * Main entry point for the echo server application. 
	 * @param args contains the port number at args[0].
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		try {
			new LogSetup("logs/client.log", Level.OFF);
			KVClient app = new KVClient();
			app.kvClientrun();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("send <text message>");
		sb.append("\t\t sends a text message to the server \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");

		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {

		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

	@Override
	public void handleNewMessage(TextMessage msg) {
		if(!stop) {
			System.out.println(msg.getMsg());
		}
	}

	@Override
	public void handleNewPostKVMessage(KVMessage msg) {
		if(!stop) {
			if (msg.getStatus().equals("PUT_ERROR")){
				System.out.println("Your request was not successful!");				
			}
			else{
				System.out.println(msg.getStatus().name()+"  < "+msg.getKey()+","+msg.getValue()+" >");
			}
		}
	}


	@Override
	public void handleNewGetKVMessage(KVMessage msg) {
		if(!stop) {
			if (msg.getStatus().equals("GET_ERROR")){
				System.out.println("Tuple not found!");				
			}
			else{
				System.out.println(msg.getStatus()+"  < "+msg.getKey()+","+msg.getValue()+" >");
			}
		}
	}


	@Override
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: " 
					+ serverAddress + " / " + serverPort);

		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " 
					+ serverAddress + " / " + serverPort);
			System.out.print(PROMPT);
		}

	}

}
