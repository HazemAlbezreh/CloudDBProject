package app_kvEcs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import config.ServerInfo;
import ecs.EcsStore;

public class MonitoringThread extends Thread {
	ECS monitoredECS;
	private HashMap<ServerInfo, EcsStore> serversConnection;

	private HashMap<ServerInfo, EcsStore> getServersConnection() {
		return serversConnection;
	}


	public MonitoringThread(ECS ecs) {
		this.monitoredECS = ecs;
		serversConnection = new HashMap<ServerInfo, EcsStore>(); 
	}

	public void run() {

		while (monitoredECS.monitoring) {
			ArrayList<ServerInfo> activeServers = new ArrayList<ServerInfo>(
					this.monitoredECS.getActiveServers());
			//System.out.println("monitoring");
			try {
				Iterator<ServerInfo> it = activeServers.iterator();
				while (it.hasNext()) {
					ServerInfo server = it.next();

					if (this.serversConnection.containsKey(server)) {
						if (!monitoredECS.heartBeatServer(server)) {
							monitoredECS.removeFailedNode(server);
							this.getServersConnection().remove(server);
						}
					} else {

						EcsStore serverSocket = new EcsStore(
								server.getServerIP(), server.getPort());
						try {
							serverSocket.connect();
							this.getServersConnection().put(server,
									serverSocket);
						} catch (Exception e) {
							monitoredECS.removeFailedNode(server);
							this.getServersConnection().remove(server);
						}
					}

				}
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
