package command.imp;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import socket.SocketClient;
import ui.Application;
import command.in.Command;;


public class SendCommand implements Command {

    private SocketClient aClient= null;	
    private String message="";
    boolean OK=false;
	private Logger logger = Logger.getLogger(SendCommand.class);; 
	public  SendCommand(SocketClient client,String[] options){
		OK=false;
		aClient=client;
		PropertyConfigurator.configure("logs/log.config");
		logger.setLevel(Application.currentLevel);
		setMessage(options);
		
	}
	
	public SendCommand() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public String execute() {
		logger.debug("Trying to send "+message);
		
		String response="";
		if(OK){
			
			aClient.send(this.message);
			logger.info("message sent");
			response=aClient.recieve();
			logger.info("response from server recieved");
			
		}else{
			
			logger.fatal("Can not send if there is no connection");
			response="Error! Not connected!\n";
			
		}
        return response;

	}

	@Override
	public String getHelpText() {
       
		String helpText="Usage: send <message> \n";
		return helpText;
	}

	
	private void setMessage(String[] options) {
		
		try {
			StringBuilder builder = new StringBuilder();
			for(String s : options) {
			    builder.append(s);
			    builder.append(" ");
			}
			builder.deleteCharAt(builder.length()-1);
			message= builder.toString();
			
			if(message!=null && !message.equals("") && aClient.isConnected())
				OK=true;
		} catch (Exception e) {
			OK=false;
			//e.printStackTrace();
		}

	}

}
