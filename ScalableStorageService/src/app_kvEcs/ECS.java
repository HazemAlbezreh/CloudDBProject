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
import client.KVStore;
import config.*;
import consistent_hashing.*;

public class ECS {

	private Config config;
	private List<ServerInfo> activeServers;
	private List<ServerInfo> inActiveServers;
	private ConsistentHash<ServerInfo> consistentHash;
	private HashMap<ServerInfo, KVStore> serversConnection;
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

	public HashMap<ServerInfo, KVStore> getServersConnection() {
		return serversConnection;
	}

	public void setServersConnection(
			HashMap<ServerInfo, KVStore> serversConnection) {
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
		serversConnection = new HashMap<ServerInfo, KVStore>();
	}
	
	public static void main(String[] args) throws IOException {
		args = new String[2];
		args[0] = "ecs.config";
		ECS application = new ECS(args[0]);
		application.getInActiveServers();
	}

}
