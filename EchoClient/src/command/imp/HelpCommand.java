package command.imp;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import ui.Application;

import command.in.Command;;

public class HelpCommand implements Command {

	enum CommandNames {unknown, connect,disconnect,logLevel,help, quit, send }
	private Command aCommand=null;
	private Logger logger = Logger.getLogger(HelpCommand.class);
	
	public HelpCommand(String commandName){
		PropertyConfigurator.configure("logs/log.config");
		logger.setLevel(Application.currentLevel);
		setCommand(commandName);
		
	}
	
	public HelpCommand(){

	}
	
	@Override
	public String execute() {
		
		logger.debug("Getting help is started");
		String helpText=aCommand.getHelpText();
		if(helpText==null) helpText=getHelpText();
		
		return helpText;
	}

	@Override
	public String getHelpText() {
		String helpText="";
		helpText+="\n========================================================================================";
		helpText+="\n		EchoClient : Command line based user interface (CLI)";
		helpText+="\n========================================================================================";
		helpText+="\n";
		helpText+="\n";
		helpText+="\nDESCRIPTION:\n a simple command line–based user interface (CLI), and the communication\n logic for interacting with the server based on TCP stream sockets in Java";
		helpText+="\n";
		helpText+="\n";
		helpText+="\nCOMMANDS:";
		helpText+="\n";
		helpText+="\n";
		helpText+="\n1- connect		Tries to establish a TCP- connection to the echo server based on the";
		helpText+="\n 			given server address and the port number of the echo service. " ;
		helpText+="\n			Usage: connect <address> <port>.";
		helpText+="\n\n2- disconnect		Tries to disconnect from the connected server.";
		helpText+="\n			Usage: disconnect.";
		helpText+="\n\n3- send			Sends a text message to the echo server according to the";
		helpText+="\n			communication protocol. ";
		helpText+="\n			Usage: send <message>.";
		helpText+="\n\n4- logLevel		Sets the logger to the specified log level.";
		helpText+="\n			Usage:logLevel <level>";
		helpText+="\n\n5- help			Usage: Command Usage => help <commandName> OR Full Help Text => help .";
		helpText+="\n6- quit			Usage: quit.\n\n\n";
		
		
		return helpText;
	}

	
	private void setCommand(String commandName) {
       
		CommandNames command=CommandNames.unknown;
		
		try{
			if(commandName==null)
				command=CommandNames.help;
			else
			   command=CommandNames.valueOf(commandName);
		}catch(Exception e){
			
			 command=CommandNames.unknown;
		}
		
		switch(command){
		
		case connect:
			aCommand= new ConnectCommand();
		break;
		case disconnect:
			aCommand= new DisConnectCommand();
		break;
		case logLevel:
			aCommand= new LogLevelCommand();
		break;
		case help:
			aCommand= new HelpCommand();
		break;
		case quit:
			aCommand= new QuitCommand();
		break;
		case unknown:
			aCommand= new UnknownCommand();
		break;
		case send:
			aCommand= new SendCommand();
		break;
		default:
			aCommand= new UnknownCommand();
		break;
		
		
		}
		
	}

}
