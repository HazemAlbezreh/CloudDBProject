package app_kvEcs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.Random;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.ClientSocketListener.SocketStatus;

import common.messages.KVMessage;
import common.messages.TextMessage;

import ecs.ClientSocketListener;



public class ECSClient implements ClientSocketListener{
	private static Logger logger = Logger.getRootLogger();
	private boolean stop = false;
	private BufferedReader stdin;
	private String filepath="ecs.config";
	private static final String PROMPT = "ECSClient> ";

	private ECS ecs = null;
	boolean success=false;



	public void ECSClientrun() throws Exception {
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

		if(tokens[0].equals("quit")) {	
			stop = true;
			shutDown();
			System.out.println(PROMPT + "Application exit!");

		}else if (tokens[0].equals("init")){
			if(tokens.length ==4){
				if((tokens[3].equals("FIFO") || tokens[3].equals("LRU") ||tokens[3].equals("LFU"))){
					printError("Invalid type of strategy!");
				}else{
					init(tokens[1],tokens[2],tokens[3]);
				}
			}
			else{
				printError("Invalid number of parameters!");
			}
		}
		else if (tokens[0].equals("start")){
			try{
				start();
			}
			catch(NullPointerException e){
				printError("You must type first init command");
			}
		}else if(tokens[0].equals("stop")) {
			try{
				stop();
			}
			catch(NullPointerException e){
				printError("You must type first init command");
			}
			catch(IOException ioe){
				printError("Error stopping servers");
			}

		} else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT +"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
		} 
		else if(tokens[0].equals("addServer")) {
			if(tokens.length ==3){
				if( (tokens[2].equals("FIFO") || tokens[2].equals("LRU") ||tokens[2].equals("LFU"))){
					printError("Invalid type of strategy!");
				}else{
					try{
						addServer(tokens[1],tokens[2]);
					}
					catch(NullPointerException e){
						printError("You must type first init command");
					}
				}
				
			}
			else{
				printError("Invalid number of parameters!");

			}
		}
		else if(tokens[0].equals("removeServer")) {
			try{
				removeServer();
			}
			catch(NullPointerException e){
				printError("You must type first init command");
			}
		}
		else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
	}


	private void removeServer() throws IOException {
		success=ecs.removeNode();
		if(success){
			System.out.println(PROMPT+"A node has been removed!");
		}
		else{
			System.out.println(PROMPT+"There was an error removing the node!");

		}
	}


	private void addServer(String cacheSize, String displacementStrategy) throws IOException{
		int cache=Integer.parseInt(cacheSize);
		
		success=ecs.addNode(cache,displacementStrategy);
		if(success){
			logger.debug("A node has been added! ");
			System.out.println(PROMPT+"A node has been added!");
		}
		else{
			System.out.println(PROMPT+"There was an error adding the node!");
			logger.debug("There was an error adding the node! ");
		}
	}


	private void stop() throws IOException{
		success=ecs.stop();
		if(success){
			logger.debug("All nodes have been stopped! ");
			System.out.println(PROMPT+"All nodes have been stopped!");
		}
		else{
			logger.debug("There was an error stopping all nodes! ");
			System.out.println(PROMPT+"There was an error stopping all nodes!");

		}
	}

	private void init(String numNodes,String cacheSize, String displacementStrategy) {
		ecs = new ECS(filepath);
		if (ecs==null){
			System.out.println(PROMPT+"Error in initialization!");
			logger.debug("There was an error initiating ECS! ");
		}
		else{
			System.out.println(PROMPT+"ECS is initialized!");
			ecs.initService(Integer.parseInt(numNodes),Integer.parseInt(cacheSize),displacementStrategy);
			logger.debug("ECS is initialized!! ");

		}
	}
	
	private void start() {
		success=ecs.start();
		if(success){
			logger.debug("All nodes have been started!! ");
			System.out.println(PROMPT+"All nodes have been started!");
		}
		else{
			logger.debug("There was an error starting all nodes! ");
			System.out.println(PROMPT+"There was an error starting all nodes!");

		}
	}


	private void shutDown() {
		if(ecs != null) {
			try{
				ecs.shutDown();
			}catch(NullPointerException ne){
				printError("There was an error shutting down ecs!");
			}
			ecs = null;
		}
	}

	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("init <numberOfNodes> <cacheSize> <FIFO | LRU | LFU>");
		sb.append("\t Initializes servers \n");
		sb.append(PROMPT).append("start");
		sb.append("\t starts the storage service\n");
		sb.append(PROMPT).append("stop");
		sb.append("\t\t stops the storage service - Servers do not process client requests but keep running \n");
		sb.append(PROMPT).append("addServer <cacheSize> <FIFO | LRU | LFU>");
		sb.append("\t adds new KVServer \n");
		sb.append(PROMPT).append("removeServer");
		sb.append("\t\t removes random KVServer \n");
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		sb.append(PROMPT).append("quit");
		sb.append("\t\t stops all server instances and exits the remote processes \n");
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



	public static void main(String[] args) throws Exception {
		try {
			new LogSetup("logs/ecsClient.log", Level.OFF);
			ECSClient app = new ECSClient();
			app.ECSClientrun();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}


	@Override
	public void handleNewMessage(TextMessage msg) {
		if(!stop) {
			System.out.println(msg.getMsg());
		}
		
	}


	@Override
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: ");

		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: ");
			System.out.print(PROMPT);
		}
		
	}


	


}
