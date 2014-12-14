package testing;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import socket.SocketWrapper;
import common.messages.ClientMessage;
import common.messages.KVMessage;
import common.messages.Message;
import common.messages.KVMessage.StatusType;
import client.KVStore;
import config.ServerInfo;
import app_kvClient.KVClient;
import app_kvEcs.ECS;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import logger.LogSetup;

public class AdditionalTest extends TestCase {
	private static ECS ecs;
	private static KVStore client;
	private static int nServers = 2;
	private static int cacheSize = 10;
	private static String strategy = "FIFO";
	private static int port=50000;
	private static String address="";

	@BeforeClass
	public static void init() {

	}
	
	public void setUp() {
		ecs = new ECS("ecs.config");
		ecs.initService(nServers,cacheSize,strategy);
	}

	public void tearDown() {
		ecs.shutDown();
	}
	
	@Test
	public void testStartAllServers() {		//TODO MUST BE CHANGED
		
		assertTrue(ecs.start());

	}
	
	
	
	@Test
	public void testRemoveServer() {     // TODO THINK IT WORKS 
		Exception ex=null;
		KVMessage reply=null;
		ecs.start();
		ecs.removeNode();
		//TODO put something from file ecs.config
		ServerInfo dc= ecs.getActiveServers().get(0);
		client=new KVStore(dc.getServerIP(), dc.getPort());
		try {
			client.connect();
		} catch (Exception e) {
			ex=e;
		}
		try {
			reply=client.get("invalidGet");
		} catch (Exception e) {
			ex=e;
		}
		assertTrue(reply.getStatus().equals(StatusType.GET_ERROR));
		
	}
	
	
	@Test
	public void testStoppedServer() {
		Exception ex=null;
		KVMessage reply=null;
		ecs.start();

		client=new KVStore(ecs.getActiveServers().get(0).getServerIP(), ecs.getActiveServers().get(0).getPort());
		
		//put something from file ecs.config
		try {
			client.connect();
		} catch (Exception e) {
			ex=e;
		}
		try {
			client.put("test","removeServer");
		} catch (Exception e) {
			ex=e;
		}
		ecs.stop();
		try {
			reply=client.get("test");
		} catch (Exception e) {
			ex=e;
		}
		
		assertTrue(reply.getStatus().equals(StatusType.SERVER_STOPPED));
		
	}
	
	

	@Test
	public void testNotResponsible() {
		Exception ex=null;
		ClientMessage answer=null;
		ecs.start();

		client=new KVStore(ecs.getActiveServers().get(0).getServerIP(), ecs.getActiveServers().get(0).getPort());
		
		//put something from file ecs.config
		try {
			client.connect();
		} catch (Exception e) {
			ex=e;
		}
		try {
			//sth that is not responsible for 1st server
			ClientMessage newmsg=new ClientMessage("___","___",StatusType.PUT);
			client.getClientSocket().sendMessage(newmsg);

			Message latestMsg = client.getClientSocket().recieveMesssage();	
			answer=(ClientMessage)latestMsg;
		} catch (Exception e) {
			ex=e;
		}
		
		assertTrue(answer.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE));
		
	}
	
	
	
	@Test
	public void testClientServer() {
		Exception ex=null;
		KVMessage reply=null;
		ClientMessage answer=null;
		ecs.start();

		client=new KVStore(ecs.getActiveServers().get(0).getServerIP(), ecs.getActiveServers().get(0).getPort());
		
		//put something from file ecs.config
		try {
			client.connect();
		} catch (Exception e) {
			ex=e;
		}
		try {
			//sth that is not responsible for 1st server
			ClientMessage newmsg=new ClientMessage("test","lololol",StatusType.PUT);
			client.getClientSocket().sendMessage(newmsg);

			Message latestMsg = client.getClientSocket().recieveMesssage();	
			answer=(ClientMessage)latestMsg;
		} catch (Exception e) {
			ex=e;
		}
		client.updateMetaData(answer.getMetadata());
		try {
			reply=client.put("test", "updatedValue");
		} catch (Exception e) {
			ex=e;
		}
		assertTrue( (reply.getStatus().equals(StatusType.PUT_UPDATE)) 
				&& client.getRing()!=null);
		
	}
	
}
