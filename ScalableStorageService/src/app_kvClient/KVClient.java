package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.ClientConnection;
import app_kvServer.ServerStatus;

import common.messages.TextMessage;

import client.ClientSocketListener.SocketStatus;
import client.KVStore;



public class KVClient {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "EchoClient> ";
	private BufferedReader stdin;
	private boolean stop = false;

	private String serverAddress;
	private int serverPort;
	private KVStore client = null;

	public static int port;
	private ServerSocket serverSocket=null;
	private boolean running;


	public KVClient(int port) {
		this.port= port;	
	}

	private boolean initialize() {
		try {
			this.serverSocket = new ServerSocket(port);
			this.running=true;
			return true;

		} catch (IOException e) {
			this.running=false;
			return false;
		}
	}


	/**
	 * kvClientrun reads the user command line and calls handleCommand to recognize the type 
	 * 
	 * @throws Exception 
	 */
	public void kvClientrun() throws Exception {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);

			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				this.running=false;
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}


	/**
	 * handleCommand is responsible for recognising the type of command  
	 * 
	 * @param cmdLine
	 * @throws Exception when problem in connection
	 */
	private void handleCommand(String cmdLine) throws Exception {
		String[] tokens = cmdLine.split("\\s+");
		String key;
		String value;

		if(tokens[0].equals("quit")) {	
			stop = true;
			this.running=false;
			disconnect();

			System.out.println(PROMPT + "Application exit!");

		}else if (tokens[0].equals("subscribe")){
			if(tokens.length==2){
				subscribe(tokens[1]);
			}else {
				printError("Invalid number of parameters!");
			}
		
		}else if (tokens[0].equals("unsubscribe")){
			if(tokens.length==2){
				unsubscribe(tokens[1]);
			}else {
				printError("Invalid number of parameters!");
			}
		
		}
		else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					connect(serverAddress, serverPort);
				}catch(NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				}
				catch (UnknownHostException e) {
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
			//the new command
			if(tokens.length ==3){
				key =tokens[1];
				//if user does not write second argument on put command, then we assume 
				//the null value aka DELETE key
				if (tokens.length==3)
					value=tokens[2];
				else
					value=null;
				try{
					putTuples(key,value);
				} catch (IOException e) {
					printError("Could not execute put command!");
					logger.warn("Could not execute put command!", e);
				}catch (NullPointerException ne){
					printError("You must be connected to a server first!");
					logger.warn("You must be connected to a server first!", ne);

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
				}catch (NullPointerException ne){
					printError("You must be connected to a server first!");
					logger.warn("You must be connected to a server first!", ne);

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


	private void unsubscribe(String key) {
		if (key.equals("null")){
			printError("Please type another key !");
		}
		else{
			logger.info("Going to subscribe to a specified key "+key);
			try {
				client.unsubscribe(key);
			} catch (Exception e) {
				printError("There was a connection error!");
				logger.info("Connection error!!");

			}
		}
		
	}

	private void subscribe(String key) {
		if (key.equals("null")){
			printError("Please type another key !");
		}
		else{
			logger.info("Going to subscribe to a specified key "+key);
			try {
				client.subscribe(key);
			} catch (Exception e) {
				printError("There was a connection error!");
				logger.info("Connection error!!");

			}
		}
		
	}

	/** Uses get function from KVStore class
	 *  
	 * @param key
	 * @throws Exception when getting the value of the key
	 */
	private void getTuples(String key) throws Exception {
		logger.info("Going to fetch value of key "+key);
		client.get(key);

	}


	/** Uses put function from KVStore class
	 *  
	 * @param key,value
	 * @throws Exception when putting the tuple key,value
	 */
	private void putTuples(String key, String value) throws Exception {
		logger.info("Going to put this pair of tuples < "+key+" , "+value+" >");
		client.put(key, value);

	}

	/** Initiates the connection with the server
	 *  
	 * @param address,port
	 * 
	 */
	private void connect(String address, int port) 
			throws Exception {
		client = new KVStore(address, port);
		TextMessage latestMsg=client.connect();
		KVStore.handleTextMessage(latestMsg);
	}


	/** disconnects from the server
	 *  
	 */
	private void disconnect() {
		if(client != null) {
			SocketStatus status=client.disconnect();
			client = null;
			KVStore.handleStatus(status, this.serverAddress,this.serverPort);
		}
	}

	/**
	 * Main entry point for the server application. 
	 * @param args contains the port number at args[0].
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		try {
			new LogSetup("logs/client.log", Level.ALL);
			int port = Integer.parseInt(args[0]);
			KVClient app = new KVClient(port);
			if(!app.initialize()){
				System.out.println("Error! Try another port!");
				System.exit(1);
			}

			app.listenForSubs();
			app.kvClientrun();

		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void listenForSubs() {
		if (this.serverSocket != null) {
			
				try {
					Subscription subscribe = new Subscription(serverSocket);
					new Thread(subscribe).start();
				} catch (IOException e) {
					if(isRunning()){
					}else{
					}					
				}
			
		}		
	}

	private synchronized boolean isRunning() {
		return this.running;
	}

	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t puts a tuple <key,value> on server \n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t gets the value stored from tuple <key,value> stored on server \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		sb.append(PROMPT).append("subscribe <key>");
		sb.append("\t\t\t subscribes on this key and gets notifications \n");
		sb.append(PROMPT).append("unsubscribe <key>");
		sb.append("\t\t\t unsubscribes on this key \n");
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






}
