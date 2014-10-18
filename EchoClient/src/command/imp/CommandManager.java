package command.imp;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import socket.SocketClient;
import ui.Application;
import command.in.Command;;

public class CommandManager {
	
	enum CommandNames {unknown, connect,disconnect,logLevel,help, quit, send }
	private Command aCommand=null;
	private SocketClient aClient= null;
	private Logger logger=Logger.getLogger(CommandManager.class);
	
	public CommandManager(){
		PropertyConfigurator.configure("logs/log.config");
		logger.setLevel(Application.currentLevel);
		aClient= new SocketClient();
	}
	
	public void ProcessCommand(String[] commandArgs) {
		
		logger.info("Process command started");
		
		CommandNames command=CommandNames.unknown;
		
		try{
			
		
			command=CommandNames.valueOf(commandArgs[0]);
		}catch(Exception e){
			
			 command=CommandNames.unknown;
		}
		
		
		
		
		
		switch(command){
		
		case connect:
			aCommand= new ConnectCommand(aClient,getOptions(commandArgs));
			
		break;
		case disconnect:
			aCommand= new DisConnectCommand(aClient);
		break;
		case logLevel:
			aCommand= new LogLevelCommand(getOptions(commandArgs));
		break;
		case help:
			String cName=null; 
			try{
				cName=commandArgs[1];
			}catch(Exception e){
				cName=null; 	
			}
			aCommand= new HelpCommand(cName);
			
		break;
		case quit:
			aCommand= new QuitCommand();
		break;
		case unknown:
			aCommand= new UnknownCommand();
		break;
		case send:
			aCommand= new SendCommand(aClient,getOptions(commandArgs));
		break;
		default:
			aCommand= new UnknownCommand();
			break;
		
		
		}
		
		
	}
	
	
	private String[] getOptions(String[] commandArgs) {
		String[] options=null;
		
		if(commandArgs.length>1){
			options =new String[commandArgs.length-1];
			for(int i=1;i<commandArgs.length;i++)
				options[i-1]=commandArgs[i];
		}
		return options;
	}




	public String execute() {
		
		return aCommand.execute();
		
	}

	public void setLoger(Logger alogger) {
		
		 logger = alogger;
	}

}
