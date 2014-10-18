package command.imp;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import ui.Application;

import command.in.Command;

public class QuitCommand implements Command {

	private Logger logger = Logger.getLogger(QuitCommand.class);


	public QuitCommand() {
		PropertyConfigurator.configure("logs/log.config");
		logger.setLevel(Application.currentLevel);
	}

	@Override
	public 	String execute() {
		
		System.out.println("Exit");
		logger.info("Exiting...");
		System.exit(0);
		return null;
		
	}

	@Override
	public String getHelpText() {
		
		return "Usage: quit.\n";
	}
	

}
