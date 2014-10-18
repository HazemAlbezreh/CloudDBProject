package command.imp;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import ui.Application;


import command.in.Command;;

public class LogLevelCommand implements Command {

	
	private Logger logger= Logger.getLogger(Application.class);
	private Level currentLevel;

	public LogLevelCommand(){
		
		
	}
	public LogLevelCommand(String[] options) {
		PropertyConfigurator.configure("logs/log.config");
		logger.setLevel(Application.currentLevel);
		try {
			currentLevel=getLevel(options[0]);
		} catch (Exception e) {
			
			currentLevel=Level.ALL;
			
		}
	}
	private Level getLevel(String inlevel) {
		
		Level inputlevel=Level.ALL;
	  try {
		 inputlevel=Level.toLevel(inlevel);
	} catch (Exception e) {
		inputlevel=Level.ALL;
		
	}	
		
		
	
		return inputlevel;
	}
	@Override
	public String execute() {
	
		logger.debug("Setting the level "+ Application.currentLevel);
		Application.currentLevel=currentLevel;
		logger.setLevel(currentLevel);
		logger.debug("Setting the level is successful");
	
	 return ""; 
	}

	@Override
	public String getHelpText() {
		// TODO Auto-generated method stub
		String helpText="Usage: logLevel <level> \n";
		return helpText;
	}


	
}
