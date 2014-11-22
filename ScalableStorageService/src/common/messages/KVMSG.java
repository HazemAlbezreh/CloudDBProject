package common.messages;

import java.io.Serializable;

import com.eclipsesource.json.JsonObject;

import common.messages.KVMessage;

public class KVMSG implements KVMessage,Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1466134465550783953L;
	private String value;
	private String key;
	private StatusType msgType ;
	private boolean error;
	
	
	public KVMSG (String key,StatusType type) {
		this.setKey(key);
		this.setStatus(type);
		this.value=null;
		this.error=false;
	}
	
	
	public KVMSG (String key,String value,StatusType type) {
		this.setKey(key);
		this.setValue(value);
		this.setStatus(type);
		this.error=false;
	}
	
	public KVMSG(String error){
		this.error=true;
		this.key="ERROR";
		this.value=error;
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

	private boolean getError(){
		return this.error;
	}
	
	@Override
	public StatusType getStatus() {
		return this.msgType;
	}
	
	private void setStatus(StatusType type) {
		this.msgType=type;
	}
	
	public static KVMSG messageParser(String s){
		String nKey;
		String nValue;
		StatusType nStatus;
		
		if(s==null || s.isEmpty()){
			return null;
		}
		try{
			JsonObject jo=JsonObject.readFrom(s);

			nStatus=StatusType.valueOf( jo.get("msgType").asString() );
			nKey=jo.get("key").asString();		
			if(jo.get("value").isNull()){
				nValue=null;
			}
			else{
				nValue=jo.get("value").asString();
			}
			if(nStatus == null){
				return null;
			}
			
			return new KVMSG(nKey,nValue,nStatus);
		}catch(Exception e){
			return null;
		}
	}
	
	public String msgToSend(){
		String result;
		JsonObject jo=new JsonObject(); 
		jo.add("msgType", this.msgType.toString());
		jo.add("key", this.key);
		jo.add("value", this.value);

		result=jo.toString().trim();
		return result;
	}

}
