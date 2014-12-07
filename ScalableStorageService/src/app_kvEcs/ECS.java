package app_kvEcs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
	private static Logger logger = Logger.getLogger(ECS.class);

	public static ECS getInstance(String filePath) {
		if (instance == null) {
			instance = new ECS(filePath);
		}
		return instance;
	}

	public ECS(String filePath) {
		this.init(filePath);
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
		this.getInActiveServers().get(0).runServerRemotly("Path");
		this.getActiveServers().add(this.getInActiveServers().get(0));
		this.getInActiveServers().remove(0);
	}

	
	
	public boolean initService(int numberOfNodes) {
		if (numberOfNodes <= this.getInActiveServers().size()) {
			runFirstServer();
			// int[] arrayList = new int[] { 2, 5 };
			Random random = new Random();
			for (int i = 0; i < numberOfNodes - 1; i++) {
				int randomIndex = random.nextInt(inActiveServers.size());// arrayList[i];
				ServerInfo server = this.getInActiveServers().get(randomIndex);
				//TODO change path
				server.runServerRemotly("Path");
				this.getInActiveServers().remove(randomIndex);
				this.getActiveServers().add(server);
			}
			this.consistentHash = new ConsistentHash<ServerInfo>(
					Md5HashFunction.getInstance(), activeServers);

			boolean result;
			result = this.connectToActiveServers();
			result &= this.updateMetadata();
			return result;

		} else {
			logger.error("Number entered is too big! Maximum allowed: "
					+ this.activeServers.size());
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
			logger.error("Couldn't connect to server: " + server);
			return false;
		}
		return true;
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
		ECSMessage response = serverSocket.updateMetaData(this.consistentHash
				.getMetaData());
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
		for (ServerInfo server : this.getActiveServers())
			shutDownServer(server);
	}

	private void shutDownServer(ServerInfo server) {
		EcsStore serverSocket = this.getServersConnection().get(server);
		serverSocket.shutDown();
		this.getInActiveServers().add(server);
		this.getActiveServers().remove(server);
	}

	public void serverMoveData(ServerInfo server, Range range,
			ServerInfo addedNode) {
		EcsStore serverSocket = this.getServersConnection().get(server);
		serverSocket.moveData(range, addedNode);
	}
	
	
	public boolean addNode(int cacheSize,String startegy) {
		// If there are idle servers in the repository, randomly pick one of
		// them
		Random random = new Random();
		int randomIndex = random.nextInt(this.getInActiveServers().size());// 0;
		ServerInfo addedNode = this.getInActiveServers().get(randomIndex);
		// send an SSH call to invoke the KVServer process.

		addedNode.runServerRemotly("path");
		this.getInActiveServers().remove(randomIndex);
		this.getActiveServers().add(addedNode);
		// Determine the position of the new storage
		// server within the ring by hashing its address
		int key = this.consistentHash.getHashFunction().hash(addedNode);
		ServerInfo successor = CommonFunctions.getSuccessorNode(key,
				this.consistentHash.getMetaData());
		// Recalculate and update the meta-data of the storage service
		this.consistentHash.add(addedNode);
		// Initialize the new storage server with the updated meta-data and
		// start it.
		this.connectToServer(addedNode);
		EcsStore addedServerSocket = this.getServersConnection().get(addedNode);
		addedServerSocket.updateMetaData(this.consistentHash.getMetaData());
		addedServerSocket.start();
		// Set write lock (lockWrite()) on the successor node
		this.lockWrite(successor);

		// Invoke the transfer of the affected data items
		ServerInfo predecessor = CommonFunctions.getPredecessorNode(key,
				this.consistentHash.getMetaData());
		int preKey = this.consistentHash.getHashFunction().hash(predecessor);
		Range range = new Range(preKey, key);
		this.serverMoveData(successor, range, addedNode);
		// Send a meta-data update to all storage servers
		this.updateMetadata();
		// Release the write lock on the successor node
		this.unLockWrite(successor);
		// TODO REMOVE UNHANDELD DATA ON THE SUCCESSOR

		return true;

	}

	public boolean removeNode() {
		// Randomly select one of the storage servers.
		Random random = new Random();
		int randomIndex = random.nextInt(this.getActiveServers().size());
		ServerInfo removedNode = this.getActiveServers().get(randomIndex);
		// Recalculate and update the meta-data of the storage service
		this.consistentHash.remove(removedNode);
		// Set the write lock on the server that has to be deleted.
		this.lockWrite(removedNode);
		// Send meta-data update to the successor node (i.e., successor is now
		// also responsible for the range of the server that is to be removed)
		int key = this.consistentHash.getHashFunction().hash(removedNode);
		ServerInfo successor = CommonFunctions.getSuccessorNode(key,
				this.consistentHash.getMetaData());
		this.updateServerMetadata(successor);
		// Invoke the transfer of the affected data items
		ServerInfo predecessor = CommonFunctions.getPredecessorNode(successor,
				this.consistentHash.getMetaData());
		int preKey = this.consistentHash.getHashFunction().hash(predecessor);
		int successorKey = consistentHash.getHashFunction().hash(successor);
		Range range = new Range(preKey, successorKey);
		// serverToRemove.moveData(range, successor)
		this.serverMoveData(removedNode, range, successor);

		this.getActiveServers().remove(randomIndex);
		this.getInActiveServers().add(removedNode);

		// When all affected data has been transferred (i.e., the server that
		// has to be removed sends back a notification to the ECS)
		// Send a meta-data update to the remaining storage servers.
		this.updateMetadata();

		// Shutdown the respective storage server.
		this.shutDownServer(removedNode);

		return true;
	}


	
	public static void main(String[] args) throws IOException {
		args = new String[2];
		args[0] = "ecs.config";
		ECS application = new ECS(args[0]);
		application.getInActiveServers();
	}

}
