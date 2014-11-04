package testing;

import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.Test;


import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import client.KVStore;

import junit.framework.TestCase;

public class AdditionalTest extends TestCase {

	private KVStore kvClient;

	public void setUp() throws UnknownHostException, IOException {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}


	@Test
	public void testdeleteNonExisting() {

		String key = "deleteNonExistingValue";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);

	}
	
	
	
	@Test
	public void testIfExistsInCache() {

		
		
		KVMessage response = null;
		KVMessage response2 = null;

		Exception ex = null;
		
		try {
			String key ="pinokio";
			String value="nose";
			response = kvClient.put(key,value);
			key ="tom";
			value="jery";
			response = kvClient.put(key,value);
			key ="tom";
			value="cat";
			response2 = kvClient.put(key,value);
			response = kvClient.get(key);


		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getValue().equals("cat") && response.getStatus()==StatusType.GET_SUCCESS && response2.getStatus()==StatusType.PUT_UPDATE);

	}
}
