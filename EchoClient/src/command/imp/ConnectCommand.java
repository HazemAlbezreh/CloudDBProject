package command.imp;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import socket.SocketClient;
import ui.Application;
import command.in.Command;
public class ConnectCommand implements Command {

	String commadCode="connect";
	String hostAddress="";
	int hostPort=0;
	boolean OK=false;
	private SocketClient aClient= null;
	private Logger logger=Logger.getLogger(ConnectCommand.class);;	
	
	public  ConnectCommand(SocketClient client,String[] options){
		PropertyConfigurator.configure("logs/log.config");
		logger.setLevel(Application.currentLevel);
		OK=false;
		aClient=client;
		setOptions(options);
		
	}
	
   public  ConnectCommand(){
		
	}
	
   
   /**
    * initiates a connection to the server
    */
	@Override
	public String execute() {
		String message="";
	if(OK){
		
		try {
			logger.debug("try to connect to"+hostAddress+" and port "+hostPort);
			aClient.connect(hostAddress, hostPort);
			logger.info("connect to"+hostAddress+" and port "+hostPort+ " is successful");
			message =aClient.recieve();
			logger.debug("data recieved from server");
		} catch (Exception e) {
			
			logger.fatal("Connection is not successful");
			e.printStackTrace();
		}
		
	 }
	
	return message;
	}
	/**
	 * Gets the usage information
	 */
	@Override
	public String getHelpText() {
		String helpText="Usage: connect <address> <port> \n";
	
		
		return helpText;
	}

	
	private void setOptions(String[] options) {
		try{
		hostAddress=options[0];
		hostPort=Integer.parseInt(options[1]);
		OK=true;
		}catch(Exception e) {
			logger.fatal("Command arguments are not applicable");
			System.out.println(getHelpText());
		}		
	}

}
