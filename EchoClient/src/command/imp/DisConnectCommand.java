package command.imp;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import socket.SocketClient;
import ui.Application;
import command.in.Command;;

public class DisConnectCommand implements Command {

	
	
    private SocketClient aClient= null;
	private Logger logger= Logger.getLogger(DisConnectCommand.class);	
	
	public  DisConnectCommand(SocketClient client){
		PropertyConfigurator.configure("logs/log.config");
		logger.setLevel(Application.currentLevel);
		aClient=client;	
	}
	
   public  DisConnectCommand(){
		
	}
	@Override
	public String execute() {
		String response="";
		
		logger.debug("Trying to disconnect");
		if(aClient.disconnect()){
			
			logger.info("Disconnection is successful");
			response="Connection terminated\n";
		}else{
			
			logger.fatal("Disconnection is unsuccessful");
			response="Unable to close connection\n";
		}
		return response;
	}

	@Override
	public String getHelpText() {
		
        String helpText="Usage: disconnect \n";
	
		
		return helpText;
	}

	
}
