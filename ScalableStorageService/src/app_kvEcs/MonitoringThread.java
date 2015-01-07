package app_kvEcs;

import config.ServerInfo;

public class MonitoringThread extends Thread{
		ECS monitoredECS;
		public MonitoringThread(ECS ecs) {
			this.monitoredECS = ecs;
		}
	    public void run() {
	        while(monitoredECS.monitoring){
	        	System.out.println("monitoring");
	        	try {
	        			for (ServerInfo server : monitoredECS.getActiveServers()) {
	        				if (!monitoredECS.heartBeatServer(server))
	        					monitoredECS.removeFailedNode(server);
	        			}	        				        		
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	    }

	    

}
