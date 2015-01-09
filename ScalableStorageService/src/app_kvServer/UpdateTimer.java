package app_kvServer;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import common.messages.ServerMessage;
import socket.SocketWrapper;
import config.ServerInfo;
import ecs.EcsStore;

public class UpdateTimer {

	Timer timer;
	Map<String,String> data=null;
	KVServer server=null;
	private static Logger logger = Logger.getRootLogger();


  public UpdateTimer(KVServer server,int seconds) {
	  data=new HashMap<String,String>();
	  this.server=server;
	  timer = new Timer();
	  timer.schedule(new UpdateTask(), seconds * 1000,seconds * 1000);
  }
  
  class UpdateTask extends TimerTask {
	    public void run() {
	      System.out.println("Time's up! : Sending Updates to replicas");
	      Map<String,String> map=getMap();
	      for(ServerInfo si : server.getReplicas() ){
	    	  try{
				EcsStore ecsStore = new EcsStore(si.getServerIP(), si.getPort());
				ecsStore.connect();
				SocketWrapper target = ecsStore.getSocketWrapper();
				ServerMessage dataMap=new ServerMessage(ServerMessage.StatusType.UPDATE_REPLICA,map);
				target.sendMessage(dataMap);
				target.disconnect();
	    	  }catch(Exception e){
//	    		  logger.error("Exception during sending updates to replicas "+e.getMessage() );
	    		  e.printStackTrace();
	    	  }
	      }
	    }
	  }
  
  synchronized void addData(String key,String value){
	  if(this.data==null){
		  this.data=new HashMap<String,String>();
	  }
	  this.data.put(key, value);
  }
  
  synchronized  Map<String,String> getMap(){
	  Map<String,String> temp= this.data;
	  this.data=new HashMap<String,String>(); ;
	  return temp;
  }
  
  public void cancelTimer(){
	  this.timer.cancel();
  }
  
  public static void main(String args[]) throws InterruptedException{
	    System.out.println("About to schedule task.");
	    UpdateTimer ut=new UpdateTimer(null,1);
	    System.out.println("Task scheduled.");
	    ut.addData("k", "f");
	    ut.addData("lol", "k");
	    Thread.sleep(4000);
	    ut.addData("k2", "f");
	    ut.addData("lol2", "k");
	    Thread.sleep(4000);
	    ut.cancelTimer();
  }
}
