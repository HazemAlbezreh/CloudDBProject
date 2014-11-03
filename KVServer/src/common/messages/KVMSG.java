package common.messages;

import java.io.Serializable;

import common.messages.KVMessage;

public class KVMSG implements KVMessage,Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1466134465550783953L;
	private String value;
	private String key;
	private StatusType msgType ;
	
	
	
	public KVMSG (String key,StatusType type) {
		this.setKey(key);
		this.setStatus(type);
	}
	
	
	public KVMSG (String key,String value,StatusType type) {
		this.setKey(key);
		this.setValue(value);
		this.setStatus(type);
	}
	
	
	
	public void setKey(String key) {
		this.key=key;
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public String getValue() {
		return this.value;
	}

	private void setValue(String value) {
		this.value=value;
	}

	@Override
	public StatusType getStatus() {
		return this.msgType;
	}
	
	private void setStatus(StatusType type) {
		this.msgType=type;
	}

}
