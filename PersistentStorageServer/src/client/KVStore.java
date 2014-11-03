package client;

import socket.SocketWrapper;
import common.messages.KVMSG;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface {

	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	private SocketWrapper socketWrapper ;
	private String host;
    private int port;
	
	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @param host the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}

	public SocketWrapper getSocketWrapper() {
		return socketWrapper;
	}

	public void setSocketWrapper(SocketWrapper socketWrapper) {
		this.socketWrapper = socketWrapper;
	}
	
	public KVStore(String address, int port) {
		this.setHost(address);
		this.setPort(port);
		this.setSocketWrapper(new SocketWrapper());
	}
	
	@Override
	public void connect() throws Exception {
		this.getSocketWrapper().connect(this.getHost(), this.getPort());
	}

	@Override
	public void disconnect() {
		this.getSocketWrapper().disconnect();
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		this.getSocketWrapper().sendMessage(new KVMSG(key,value,StatusType.PUT));
		
		return this.getSocketWrapper().recieveKVMesssage();
	}

	@Override
	public KVMessage get(String key) throws Exception {
		this.getSocketWrapper().sendMessage(new KVMSG(key, StatusType.GET));
		return this.getSocketWrapper().recieveKVMesssage();
	}

	
}
