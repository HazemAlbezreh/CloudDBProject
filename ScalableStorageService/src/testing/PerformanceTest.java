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

	private static int nClients = 1;
	private static int nServers = 1;
	private static long nMessages = 10;
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

	/*@Test
	public void testPut() throws Exception {
		List<KVStore> clients = new ArrayList<KVStore>(nClients);
		Random random = new Random();

		for (int i = 0; i < nClients; i++) {

			int randomIndex = random.nextInt(ecs.getActiveServers().size());
			ServerInfo serverInfo = ecs.getActiveServers().get(randomIndex);

			KVStore client = new KVStore(serverInfo.getServerIP(), serverInfo.getPort());
			client.connect();
			clients.add(client);
		}
		List<Long> latencies = new ArrayList<Long>(nMessages);
		for (int i = 0; i < nMessages; i++) {
			String key = this.randomStringGenerator(true);
			String value = this.randomStringGenerator(false);
			KVStore randomClient = clients.get(random.nextInt(clients.size()));
			long start = System.currentTimeMillis();
			randomClient.put(key, value);
			long end = System.currentTimeMillis();
			latencies.add((end - start));
		}
		Collections.sort(latencies);
		System.out.println("Best case: " + latencies.get(0) + "ms");
		System.out.println("Worst case: " + latencies.get(latencies.size() - 1)
				+ "ms");
		System.out.println("Size: " + latencies.size());

		long LatencySum=0;

		for (int i=0;i<latencies.size();i++){
			LatencySum+= latencies.get(i);
		}
		System.out.println("Latency  for 100 messages: " + LatencySum);

		System.out.println("Throughput= MAX_Num_Commands / Total_Time \n ===>"+nMessages/LatencySum);

	}

	@Test
	public void testGet() throws Exception {
		List<KVStore> clients = new ArrayList<KVStore>(nClients);
		Random random = new Random();

		for (int i = 0; i < nClients; i++) {

			int randomIndex = random.nextInt(ecs.getActiveServers().size());
			ServerInfo serverInfo = ecs.getActiveServers().get(randomIndex);

			KVStore client = new KVStore(serverInfo.getServerIP(), serverInfo.getPort());
			client.connect();
			clients.add(client);
		}
		List<Long> latencies = new ArrayList<Long>(nMessages);
		for (int i = 0; i < nMessages; i++) {
			String key = this.randomStringGenerator(true);
			String value = this.randomStringGenerator(false);
			KVStore randomClient = clients.get(random.nextInt(clients.size()));
			long start = System.currentTimeMillis();
			randomClient.get(key);
			long end = System.currentTimeMillis();
			latencies.add((end - start));
		}
		Collections.sort(latencies);
		System.out.println("Best case: " + latencies.get(0) + "ms");
		System.out.println("Worst case: " + latencies.get(latencies.size() - 1)
				+ "ms");
		System.out.println("Size: " + latencies.size());

		long LatencySum=0;

		for (int i=0;i<latencies.size();i++){
			LatencySum+= latencies.get(i);
		}
		System.out.println("Latency  for 100 messages: " + LatencySum);

		System.out.println("Throughput= MAX_Num_Commands / Total_Time \n ===>"+nMessages/LatencySum);

	}
*/

	@Test
	public void testPutAndGet() throws Exception {
		List<KVStore> clients = new ArrayList<KVStore>(nClients);
		Random random = new Random();

		for (int i = 0; i < nClients; i++) {

			int randomIndex = random.nextInt(ecs.getActiveServers().size());
			ServerInfo serverInfo = ecs.getActiveServers().get(randomIndex);

			KVStore client = new KVStore(serverInfo.getServerIP(), serverInfo.getPort());
			client.connect();
			clients.add(client);
		}
		List<Long> latencies = new ArrayList<Long>((int)nMessages);
		List<String> keys = new ArrayList<String>((int)nMessages);

		for (int i = 0; i < nMessages; i++) {
			String key = this.randomStringGenerator(true);
			String value = this.randomStringGenerator(false);
			//store each key inserted for later get

			KVStore randomClient = clients.get(random.nextInt(clients.size()));
			int putorget=random.nextInt(1); 

			int chooseRandomKey=random.nextInt(100); 

			long start = System.currentTimeMillis();
			boolean firsttime=true;
			if (firsttime){
				keys.add(key);
				randomClient.put(key,value);
				firsttime=false;
			}
			else{
				if(putorget==0){
					keys.add(key);
					randomClient.put(key,value);
				}
				else{
					//choose a random existing key
					randomClient.get(keys.get(chooseRandomKey));
				}
			}
			long end = System.currentTimeMillis();
			latencies.add((end - start));
		}
		Collections.sort(latencies);
		System.out.println("Best case: " + latencies.get(0) + "ms");
		System.out.println("Worst case: " + latencies.get(latencies.size() - 1)
				+ "ms");
		System.out.println("Size: " + latencies.size());

		long LatencySum=0;

		for (int i=0;i<latencies.size();i++){
			LatencySum+= latencies.get(i);
		}
		float result = ((float)nMessages/LatencySum);
		float AverageLatency = ((float)LatencySum/nMessages);
		System.out.println("Average Latency  for 100 messages: " + AverageLatency);

		System.out.println("Throughput= MAX_Num_Commands / Total_Time ===>"+result);

	}

}
