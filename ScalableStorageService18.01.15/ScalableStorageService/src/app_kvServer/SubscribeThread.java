package app_kvServer;

import java.util.List;

import common.messages.ClientMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import socket.SocketWrapper;
import config.ServerInfo;

public class SubscribeThread extends Thread{

	List<ServerInfo> clientInfo;
	String key=null;
	String value=null;
	
	
	public SubscribeThread(List<ServerInfo> ci,String k,String v){
		this.clientInfo=ci;
		this.key=k;
		this.value=v;
	}
	
    public void run() {
    	for(ServerInfo si : this.clientInfo){
    		try{
	    		SocketWrapper sw=new SocketWrapper();
	    		sw.connect(si.getServerIP(), si.getPort());
	    		KVMessage.StatusType status=StatusType.SUBSCRIBE_UPDATE;
	    		ClientMessage cm=new ClientMessage(this.key, this.value, status);
	    		sw.sendMessage(cm);
	    		sw.disconnect();
    		}catch(Exception e){
    			
    		}
    	}
    }

}
