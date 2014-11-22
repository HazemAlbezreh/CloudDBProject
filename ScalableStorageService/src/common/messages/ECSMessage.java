package common.messages;

import java.io.Serializable;
import java.util.SortedMap;
import config.ServerInfo;
import consistent_hashing.Range;

public class ECSMessage implements ConfigMessage,Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2046485733689892863L;
	private Range range;
	private ServerInfo server;
	private StatusType msgType ;
	private SortedMap<Integer, ServerInfo> ring;
	
	
	public ECSMessage(StatusType type){
		this.setStatus(type);
	}
	
	public ECSMessage(Range range,ServerInfo server,StatusType type){
		this(type);
		this.setRange(range);
		this.setServerInfo(server);
	}
	
	public ECSMessage(SortedMap<Integer, ServerInfo> ring,StatusType type) {
		this(type);
		this.setRing(ring);
	}
	
	@Override
	public SortedMap<Integer,ServerInfo> getRing() {
		return ring;
	}

	@Override
	public Range getRange() {
		return range;
	}

	/**
	 * @return the server info
	 */
	
	@Override
	public ServerInfo getServerInfo() {
		return server;
	}

	@Override
	public StatusType getStatus() {
		return msgType;
	}

	/**
	 * @param range the range to set
	 */
	public void setRange(Range range) {
		this.range = range;
	}

	/**
	 * @param serverInfo the server to set
	 */
	public void setServerInfo(ServerInfo server) {
		this.server = server;
	}

	/**
	 * @param msgType the msgType to set
	 */
	public void setStatus(StatusType msgType) {
		this.msgType = msgType;
	}

	/**
	 * @param ring the ring to set
	 */
	public void setRing(SortedMap<Integer, ServerInfo> ring) {
		this.ring = ring;
	}

}
