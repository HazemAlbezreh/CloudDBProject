package ecs;

import java.util.SortedMap;

import common.messages.ECSMessage;
import common.messages.KVMessage;
import config.ServerInfo;
import consistent_hashing.Range;

public interface EcsCommInterface {

	/**
	 * Establishes a connection to the KV Server.
	 * 
	 * @throws Exception
	 *             if connection could not be established.
	 */
	public void connect() throws Exception;

	/**
	 * disconnects the client from the currently connected server.
	 */
	public void disconnect();

	public ECSMessage initServer(SortedMap<Integer, ServerInfo> ring,Range range,Range rep,int cacheSize,String strategy);
	
	public ECSMessage updateMetaData(SortedMap<Integer,ServerInfo> ring,Range range,Range rep);
	
	public ECSMessage start();
	
	public ECSMessage heartBeat();
	
	public ECSMessage recoverData(SortedMap<Integer, ServerInfo> ring,Range recoverRange);
	
	public ECSMessage stop();
	
	public ECSMessage lockWrite();
	
	public ECSMessage unLockWrite();
	
	public ECSMessage moveData(Range range,ServerInfo targetServer,ECSMessage.MoveCaseType moveDataCase);
	
	public void shutDown();
}
