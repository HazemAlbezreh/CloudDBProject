package testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import config.ServerInfo;

import client.KVStore;

import app_kvEcs.ECS;

public class PerformanceTest {

	private static int nClients = 10;
	private static int nServers = 5;
	private static int nMessages = 100;
	private static int cacheSize = 10;
	private static String strategy = "FIFO";
	private static ECS ecs;
	private static Random random;
	private static List<Integer> charList;

	@BeforeClass
	public static void init() {
		try {
			new LogSetup("logs/testing/test.log", Level.ALL);
			ecs = new ECS("ecs.config");
			ecs.initService(nServers,cacheSize,strategy);
			ecs.start();
			random = new Random();
			charList = new ArrayList<Integer>(62);
			for (int i = 48; i <= 57; i++) {
				charList.add(i);
			}
			for (int i = 65; i <= 90; i++) {
				charList.add(i);
			}
			for (int i = 97; i <= 122; i++) {
				charList.add(i);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String randomStringGenerator(boolean isKey) {
		String randomString = "";
		int length;
		if (isKey) {
			length = random.nextInt(19) + 1;
		} else {
			length = random.nextInt(999) + 1;
		}
		for (int i = 0; i < length; i++) {
			Collections.shuffle(charList);
			int charValue = charList.get(random.nextInt(charList.size()));
			randomString += (char) charValue;
		}
		return randomString;
	}

	@Test
	public void test() throws Exception {
		List<KVStore> clients = new ArrayList<KVStore>(nClients);
		Random random = new Random();
		
		for (int i = 0; i < nClients; i++) {
			
			int randomIndex = random.nextInt(ecs.getActiveServers().size());
			ServerInfo serverInfo = ecs.getActiveServers().get(randomIndex);
			
			KVStore client = new KVStore(serverInfo.getServerIP(), serverInfo.getPort());
			client.connect();
			clients.add(client);
		}
		List<Integer> latencies = new ArrayList<Integer>(nMessages);
		for (int i = 0; i < nMessages; i++) {
			String key = this.randomStringGenerator(true);
			String value = this.randomStringGenerator(false);
			KVStore randomClient = clients.get(random.nextInt(clients.size()));
			long start = System.currentTimeMillis();
			randomClient.put(key, value);
			long end = System.currentTimeMillis();
			latencies.add((int) (end - start));
		}
		Collections.sort(latencies);
		System.out.println("Best case: " + latencies.get(0) + "ms");
		System.out.println("Worst case: " + latencies.get(latencies.size() - 1)
				+ "ms");
		System.out.println("Size: " + latencies.size());
	}

}
