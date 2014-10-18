package command.imp;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import ui.Application;

import command.in.Command;
import command.imp.HelpCommand;

public class UnknownCommand implements Command {

	HelpCommand aCom= new HelpCommand();
	private Logger logger = Logger.getLogger(UnknownCommand.class);;

	public UnknownCommand() {
		PropertyConfigurator.configure("logs/log.config");
		logger.setLevel(Application.currentLevel);
	}

	@Override
	public String execute() {
		logger.fatal("Unknown command");
		
		return "Unknown command\n"+aCom.getHelpText();

	}

	@Override
	public String getHelpText() {
		String helpText=" help: no help topics were found\n";
		return helpText;
	}


	
}
