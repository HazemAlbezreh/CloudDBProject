package testing;

import java.net.UnknownHostException;

import org.junit.BeforeClass;

import app_kvEcs.ECS;
import client.KVStore;
import junit.framework.TestCase;


public class ConnectionTest extends TestCase {
	private static ECS ecs;
	private static int nServers = 1;
	private static int cacheSize = 10;
	private static String strategy = "FIFO";

	
	public void setUp() {
		ecs = new ECS("ecs.config");
		ecs.initService(nServers,cacheSize,strategy);
		ecs.start();
	}
	
	public void tearDown() {
		ecs.shutDown();
	}

	
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}
	
	

	
}

