package config;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class FileReader {
	private File ecsFile;
	public FileReader(String filePath){
		this.ecsFile=new File(filePath);
	}
	
	public Config readConfiguration(){
		Config config=null;
	    try {
	    	ServerList  servers= new ServerList();
	        Scanner sc = new Scanner(this.ecsFile);
	        while (sc.hasNextLine()) {
	           ServerInfo server = parseLine(sc.nextLine());
	           servers.getServersList().add(server);
	        }
	        sc.close();
	        if (servers.server.size() > 0){
	        	config = new Config();
	        	config.servers = servers;
	        }
	        
	    } 
	    catch (FileNotFoundException e) {
	        e.printStackTrace();
	    }
	    return config;
	}
	
	private ServerInfo parseLine(String line){
		ServerInfo server=new ServerInfo();
		String[] tokens = line.split(" ");
		server.setServerIP(tokens[1]);
		try {
		server.setPort( Integer.parseInt(tokens[2]));
		}catch (NumberFormatException  ex){
			return null;
		}
		return server;
	}
	
}
