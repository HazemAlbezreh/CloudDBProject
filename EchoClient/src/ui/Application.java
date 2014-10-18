package ui;



import command.imp.CommandManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Application {

	/**
	 * @param args
	 */
	private static  Logger logger = Logger.getLogger(Application.class);
    public static Level currentLevel=Level.ALL;
    public static void main(String argv[])  {
    	logger.setLevel(currentLevel);
    	PropertyConfigurator.configure("logs/log.config");
    	logger.info("Start Echo client Application");
    	
    	BufferedReader cons = new BufferedReader(new InputStreamReader(System.in));
    	CommandManager cmdManger= new CommandManager();
    	cmdManger.setLoger(logger);
    	
    	boolean quit = false;
    	while (!quit) {
    			System.out.print("Client> "); 
    			String input="";
				try {
					input = cons.readLine();
				} catch (IOException e) {
					
					e.printStackTrace();
				} 
    			String[] tokens = input.trim().split("\\s+");
    			
    			if(!tokens[0].equals("") ){
    			  cmdManger.ProcessCommand(tokens);
    			  String message=cmdManger.execute();
    			  if(!message.equals(""))
    			  System.out.print("Client> "+message);
    			}
    	}

    }


	
	
}
