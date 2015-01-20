package app_kvEcs;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import common.messages.ConfigMessage;
import common.messages.ECSMessage;
import config.*;
import consistent_hashing.*;
import ecs.EcsStore;

public class ECS {

	private Config config;
	private List<ServerInfo> activeServers;
	private List<ServerInfo> inActiveServers;
	private ConsistentHash<ServerInfo> consistentHash;
	private HashMap<ServerInfo, EcsStore> serversConnection;
	private static ECS instance;
//	private static Logger logger = Logger.getLogger(ECS.class);
	private static final String path = System.getProperty("user.dir");
	private MonitoringThread monitoringThread;
	public boolean monitoring;
	private int defaultCacheSize;
	private String defaultCacheStrategy;

	public static ECS getInstance(String filePath) {
		if (instance == null) {
			instance = new ECS(filePath);
		}
		return instance;
	}

	public ECS(String filePath) {
		this.init(filePath);
		this.monitoring = true;
		this.monitoringThread = new MonitoringThread(this);
		this.monitoringThread.start();
	}
	
	public void setMonitoring(boolean monitoring){
		this.monitoring = monitoring ;
	}
	
	public boolean getMonitoring(){
		return this.monitoring;
	}

	public List<ServerInfo> getActiveServers() {
		return activeServers;
	}

	public void setActiveServers(List<ServerInfo> activeServers) {
		this.activeServers = activeServers;
	}

	public List<ServerInfo> getInActiveServers() {
		return inActiveServers;
	}

	public void setInActiveServers(List<ServerInfo> inActiveServers) {
		this.inActiveServers = inActiveServers;
	}

	public HashMap<ServerInfo, EcsStore> getServersConnection() {
		return serversConnection;
	}

	public void setServersConnection(
			HashMap<ServerInfo, EcsStore> serversConnection) {
		this.serversConnection = serversConnection;
	}

	public ConsistentHash<ServerInfo> getConsistentHash() {
		return consistentHash;
	}

	private void init(String filePath) {
		ConfigReader reader = new ConfigReader(filePath);
		this.config = reader.getConfig();
		activeServers = new ArrayList<ServerInfo>();
		inActiveServers = new ArrayList<ServerInfo>(this.config.getServers()
				.getServersList());
		serversConnection = new HashMap<ServerInfo, EcsStore>();
	}

	private void runFirstServer() {
		if (this.getInActiveServers().get(0).runServerRemotly(path)) {
			this.getActiveServers().add(this.getInActiveServers().get(0));
			this.getInActiveServers().remove(0);
		}
	}

	public boolean initService(int numberOfNodes, int cacheSize, String strategy) {
		if (numberOfNodes <= this.getInActiveServers().size()) {
			this.defaultCacheSize = cacheSize;
			this.defaultCacheStrategy = strategy;
			runFirstServer();
			Random random = new Random();
			for (int i = 0; i < numberOfNodes - 1; i++) {
				//int randomIndex = random.nextInt(inActiveServers.size());// arrayList[i];
				ServerInfo server = this.getInActiveServers().get(i);
				System.out.println("server " + i + " " + server.getPort());
				if (server.runServerRemotly(path)) {
					this.getInActiveServers().remove(i);
					this.getActiveServers().add(server);
				}
			}
			this.consistentHash = new ConsistentHash<ServerInfo>(
					Md5HashFunction.getInstance(), activeServers);

			boolean result;
			result = this.connectToActiveServers();
			result &= this.initServers(cacheSize, strategy);
			return result;

		} else {
//			logger.error("Number entered is too big! Maximum allowed: "
//					+ this.activeServers.size());
			return false;
		}
	}

	private boolean connectToActiveServers() {
		for (ServerInfo server : this.getActiveServers()) {
			if (!connectToServer(server)) {
				return false;
			}
		}
		return true;
	}

	private boolean connectToServer(ServerInfo server) {
		EcsStore serverSocket = new EcsStore(server.getServerIP(),
				server.getPort());
		try {
			serverSocket.connect();
			this.getServersConnection().put(server, serverSocket);
		} catch (Exception e) {
//			logger.error("Couldn't connect to server: " + server);
			return false;
		}
		return true;
	}

	private boolean initServers(int cacheSize, String strategy) {
		boolean initAllSuccess = true;
		for (ServerInfo server : this.getActiveServers()) {
			if (!initServer(server, cacheSize, strategy))
				return false;
		}
		return initAllSuccess;
	}

	private boolean initServer(ServerInfo server, int cacheSize, String strategy) {
		boolean initSuccess = true;
		EcsStore serverSocket = this.getServersConnection().get(server);
		Range range = this.getServerRange(server);
		Range rep = this.getServerReplicaRange(server);
		ECSMessage response = serverSocket.initServer(
				this.consistentHash.getMetaData(), range, rep, cacheSize,
				strategy);
		if (response == null) {
			return false;
		} else if (response.getStatus() != ConfigMessage.StatusType.INIT_SUCCESS) {
			return false;
		}
		return initSuccess;
	}

	private Range getServerRange(ServerInfo server) {
		int key = this.consistentHash.getHashFunction().hash(server);
		ServerInfo predecessor = CommonFunctions.getPredecessorNode(server,
				this.consistentHash.getMetaData());
		int preKey = this.consistentHash.getHashFunction().hash(predecessor);
		return new Range(preKey, key);
	}

	private Range getServerReplicaRange(ServerInfo server) {
		Range range = null;

		int key = this.consistentHash.getHashFunction().hash(server);

		if (this.consistentHash.getMetaData().size() == 2) {
			ServerInfo predecessor = CommonFunctions.getPredecessorNode(server,
					this.consistentHash.getMetaData());
			int preKey = this.consistentHash.getHashFunction()
					.hash(predecessor);
			range = new Range(key, preKey);
		} else if (this.consistentHash.getMetaData().size() >= 3) {

			ServerInfo predecessor = CommonFunctions.getPredecessorNode(server,
					this.consistentHash.getMetaData());
			int preKey = this.consistentHash.getHashFunction()
					.hash(predecessor);

			ServerInfo secondPredecessor = CommonFunctions.getPredecessorNode(
					predecessor, this.consistentHash.getMetaData());
			int secondPreKey = this.consistentHash.getHashFunction().hash(
					secondPredecessor);

			ServerInfo thirdPredecessor = CommonFunctions.getPredecessorNode(
					secondPredecessor, this.consistentHash.getMetaData());
			int thirdPreKey = this.consistentHash.getHashFunction().hash(
					thirdPredecessor);

			range = new Range(thirdPreKey, preKey);
		}

		return range;
	}

	private boolean updateMetadata() {
		boolean updateAllSuccess = true;
		for (ServerInfo server : this.getActiveServers()) {
			if (!updateServerMetadata(server))
				return false;
		}
		return updateAllSuccess;
	}

	private boolean updateServerMetadata(ServerInfo server) {
		boolean updateSuccess = true;
		EcsStore serverSocket = this.getServersConnection().get(server);
		Range range = this.getServerRange(server);
		Range rep = this.getServerReplicaRange(server);
		ECSMessage response = serverSocket.updateMetaData(
				this.consistentHash.getMetaData(), range, rep);
		if (response == null) {
			return false;
		} else if (response.getStatus() != ConfigMessage.StatusType.UPDATE_META_DATA_SUCCESS) {
			return false;
		}
		return updateSuccess;
	}

	public boolean start() {
		boolean startAllSuccess = true;
		for (ServerInfo server : this.getActiveServers()) {
			if (!startServer(server))
				return false;
		}
		return startAllSuccess;
	}

	private boolean startServer(ServerInfo server) {
		boolean startSuccess = true;
		EcsStore serverSocket = this.getServersConnection().get(server);
		ECSMessage response = serverSocket.start();
		if (response == null) {
			return false;
		} else if (response.getStatus() != ConfigMessage.StatusType.START_SUCCESS) {
			return false;
		}
		return startSuccess;
	}

	public boolean stop() {
		boolean stopAllSuccess = true;
		for (ServerInfo server : this.getActiveServers()) {
			if (!stopServer(server))
				return false;
		}
		return stopAllSuccess;
	}

	public boolean heartBeatServer(ServerInfo server) {
		boolean heartBeatSuccess = true;
		EcsStore serverSocket = this.getServersConnection().get(server);
		ECSMessage response = serverSocket.heartBeat();
		if (response == null) {
			return false;
		} else if (response.getStatus() != ConfigMessage.StatusType.HEART_BEAT_ALIVE) {
			return false;
		}
		return heartBeatSuccess;
	}

	private boolean stopServer(ServerInfo server) {
		boolean stopSuccess = true;
		EcsStore serverSocket = this.getServersConnection().get(server);
		ECSMessage response = serverSocket.stop();
		if (response == null) {
			return false;
		} else if (response.getStatus() != ConfigMessage.StatusType.STOP_SUCCESS) {
			return false;
		}
		return stopSuccess;
	}

	public boolean lockWrite(ServerInfo server) {
		boolean lockWriteSuccess = true;
		EcsStore serverSocket = this.getServersConnection().get(server);
		ECSMessage response = serverSocket.lockWrite();
		if (response == null) {
			return false;
		} else if (response.getStatus() != ConfigMessage.StatusType.LOCK_WRITE_SUCCESS) {
			return false;
		}
		return lockWriteSuccess;
	}

	public boolean unLockWrite(ServerInfo server) {
		boolean unLockWriteSuccess = true;
		EcsStore serverSocket = this.getServersConnection().get(server);
		ECSMessage response = serverSocket.unLockWrite();
		if (response == null) {
			return false;
		} else if (response.getStatus() != ConfigMessage.StatusType.UN_LOCK_WRITE_SUCCESS) {
			return false;
		}
		return unLockWriteSuccess;
	}

	public void shutDown() {
		for (Iterator<ServerInfo> i = this.getActiveServers().iterator(); i
				.hasNext();) {
			ServerInfo server = i.next();
			EcsStore serverSocket = this.getServersConnection().get(server);
			serverSocket.shutDown();
			this.getInActiveServers().add(server);
			this.getServersConnection().remove(server);
			i.remove();
		}
	}

	private void shutDownServer(ServerInfo server) {
		EcsStore serverSocket = this.getServersConnection().get(server);
		serverSocket.shutDown();
		if (!this.getInActiveServers().contains(server))
			this.getInActiveServers().add(server);
		if (this.getActiveServers().contains(server))
			this.getActiveServers().remove(server);
		this.getServersConnection().remove(server);
	}

	public void serverMoveData(ServerInfo server, Range range,
			ServerInfo addedNode, ECSMessage.MoveCaseType moveDataCase) {
		EcsStore serverSocket = this.getServersConnection().get(server);
		serverSocket.moveData(range, addedNode, moveDataCase);
	}

	public boolean addNode(int cacheSize, String strategy) {
		// If there are idle servers in the repository, randomly pick one of
		// them
		if (this.getInActiveServers().size() < 1)
			return false;
		// Randomly select one of the storage servers.
		Random random = new Random();
		int randomIndex = random.nextInt(this.getInActiveServers().size());
		ServerInfo addedNode = this.getInActiveServers().get(randomIndex);
		
		System.out.println("added node" + addedNode);
		// send an SSH call to invoke the KVServer process.

		if (addedNode.runServerRemotly(path)) {
			this.getInActiveServers().remove(addedNode);

			// Determine the position of the new storage
			// server within the ring by hashing its address
			ServerInfo successor = CommonFunctions.getSuccessorNode(addedNode,
					this.consistentHash.getMetaData());
			// Recalculate and update the meta-data of the storage service
			this.consistentHash.add(addedNode);
			// Initialize the new storage server with the updated meta-data and
			// start it.
			this.connectToServer(addedNode);
			EcsStore addedServerSocket = this.getServersConnection().get(
					addedNode);
			Range addedServerRange = this.getServerRange(addedNode);

			Range addedServerRepRange = this.getServerReplicaRange(addedNode);

			addedServerSocket.initServer(this.consistentHash.getMetaData(),
					addedServerRange, addedServerRepRange, cacheSize, strategy);
			addedServerSocket.start();
			// Set write lock (lockWrite()) on the successor node
			this.lockWrite(successor);

			// Invoke the transfer of the affected data items
			this.serverMoveData(successor, this.getServerRange(addedNode),
					addedNode, ECSMessage.MoveCaseType.ADD_NODE);
			// Send a meta-data update to all storage servers
			this.updateMetadata();
			// Release the write lock on the successor node
			this.unLockWrite(successor);
			this.getActiveServers().add(addedNode);
		}
		return true;
	}

	public boolean removeNode() {
		if (this.getActiveServers().size() < 1)
			return false;
		// Randomly select one of the storage servers.
		Random random = new Random();
		int randomIndex = random.nextInt(this.getActiveServers().size());
		ServerInfo removedNode = this.getActiveServers().get(randomIndex);
		
		System.out.println("removed node" + removedNode);
		// Recalculate and update the meta-data of the storage service
		this.consistentHash.remove(removedNode);
		// Set the write lock on the server that has to be deleted.
		this.lockWrite(removedNode);
		// Send meta-data update to the successor node (i.e., successor is now
		// also responsible for the range of the server that is to be removed)
		ServerInfo successor = CommonFunctions.getSuccessorNode(removedNode,
				this.consistentHash.getMetaData());
		this.updateServerMetadata(successor);
		// Invoke the transfer of the affected data items
		// serverToRemove.moveData(range, successor)
		this.serverMoveData(removedNode, this.getServerRange(successor),
				successor, ECSMessage.MoveCaseType.DELETE_NODE);

		this.getActiveServers().remove(removedNode);
		this.getInActiveServers().add(removedNode);
		

		// When all affected data has been transferred (i.e., the server that
		// has to be removed sends back a notification to the ECS)
		// Send a meta-data update to the remaining storage servers.
		this.updateMetadata();

		// Shutdown the respective storage server.
		this.shutDownServer(removedNode);

		return true;
	}

	private boolean recoverFailedNodeData(ServerInfo successor, Range range) {
		boolean recoverDataSuccess = true;
		EcsStore serverSocket = this.getServersConnection().get(successor);
		ECSMessage response = serverSocket.recoverData(
				this.consistentHash.getMetaData(), range);
		if (response == null) {
			return false;
		} else if (response.getStatus() != ConfigMessage.StatusType.RECOVER_FAILD_NODE_SUCCESS) {
			return false;
		}
		return recoverDataSuccess;
	}

	public boolean removeFailedNode(ServerInfo server) {
		if (this.getActiveServers().size() < 1)
			return false;

		Range recoverRange = this.getServerRange(server);

		// Recalculate and update the meta-data of the storage service
		this.consistentHash.remove(server);

		// Send meta-data update to the successor node (i.e., successor is now
		// also responsible for the range of the server that is to be removed)
		ServerInfo successor = CommonFunctions.getSuccessorNode(server,
				this.consistentHash.getMetaData());

		this.recoverFailedNodeData(successor, recoverRange);

		this.getActiveServers().remove(server);
		this.getInActiveServers().add(server);
		this.getServersConnection().remove(server);
		// When all affected data has been transferred (i.e., the server that
		// has to be removed sends back a notification to the ECS)
		// Send a meta-data update to the remaining storage servers.
		this.updateMetadata();
		this.addNode(defaultCacheSize, defaultCacheStrategy);
		return true;
	}

	public static void main(String[] args) throws IOException {
		args = new String[2];
		args[0] = "ecs.config";
		ECS application = new ECS(args[0]);
		application.initService(1, 10, "FIFO");


		application.start();
		System.out.println("Second node is being added");
		application.addNode(10, "FIFO");

		System.out.println("Third node is being added");
		// application.removeNode();
		application.addNode(10, "FIFO");
		
		System.out.println("Fourth node is being added");
		application.addNode(10, "FIFO");
		

		// application.removeNode();
		System.out.println("Fifth node is being added");
		application.addNode(10, "FIFO");
		System.out.println("Sixth node is being added");
		application.addNode(10, "FIFO");

//		application.removeNode();
//		application.addNode(10, "FIFO");
//
//		application.addNode(10, "FIFO");
//		application.removeNode();
//		for(int i=0;i < 100; i++){
//			System.out.println(i);
//			try {
//				Thread.sleep(3000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				
//			}
//			application.addNode(10, "FIFO");
//			application.removeNode();
//		}

		for (Map.Entry<Integer, ServerInfo> entry : application.consistentHash
				.getMetaData().entrySet()) {
			System.out.println("Server key " + entry.getKey() + " Server port "
					+ ((ServerInfo) entry.getValue()).getPort());
		}

		// application.addNode(10, "FIFO");
		application.getInActiveServers();
		application.setMonitoring(false);
		application.shutDown();
	}

}
