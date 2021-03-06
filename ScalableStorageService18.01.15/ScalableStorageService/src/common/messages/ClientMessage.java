package common.messages;

import java.io.Serializable;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import common.messages.Message;
import config.*;
public class ClientMessage implements KVMessage,Message,Serializable{

	private static final long serialVersionUID = -1466134465550783953L;
	
	private MessageType messageType= Message.MessageType.KVMESSAGE;		// Message IF

	private String value=null;												// KVMessage IF
	private String key=null;
	private StatusType statusType ;	
	private SortedMap<Integer, ServerInfo> metadata=null;
	
	private int port=-1;
	private int rights=-1;
	
	public ClientMessage(String key,String value,KVMessage.StatusType type) {
		this.setKey(key);
		this.setValue(value);
		this.setStatus(type);
		this.metadata=null;
	}
	
	public ClientMessage(SortedMap<Integer, ServerInfo> ring) {
		this.setStatus(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
		this.metadata=ring;
	}
	
	public ClientMessage(KVMessage.StatusType type) {
		this.statusType=type;
	}
	
	public ClientMessage(String key,KVMessage.StatusType type) {
		this.setKey(key);
		this.setStatus(type);
		this.value=null;
	}
	
	public ClientMessage(KVMessage.StatusType type,String key,int p){
		this.statusType=type;
		this.key=key;
		this.port=p;
	}
	
	public ClientMessage(KVMessage.StatusType type,String key,String value,int p){
		this.statusType=type;
		this.key=key;
		this.port=p;
		this.value=value;
	}
	
	@Override
	public MessageType getMessageType() {
		return this.messageType;
	}

	@Override
	public String getKey() {
		return this.key;
	}
	
	public void setKey(String k){
		this.key=k;
	}

	@Override
	public String getValue() {
		return this.value;
	}
	
	public void setValue(String v){
		this.value=v;
	}

	@Override
	public StatusType getStatus() {
		return this.statusType;
	}
	
	public void setStatus(KVMessage.StatusType st){
		this.statusType=st;
	}

	@Override
	public SortedMap<Integer, ServerInfo> getMetadata(){
		return this.metadata;
	}
	
	public int getPort(){
		return this.port;
	}
	
	@Override
	public String getJson() {
		String result;
		JsonObject jo=new JsonObject();
		jo.add("messageType", this.messageType.toString());
		jo.add("statusType", this.statusType.toString());
		jo.add("key", this.key);
		jo.add("value", this.value);
		jo.add("port", this.port);
		if(this.metadata != null){
			JsonArray ja= new JsonArray();
			for(Map.Entry<Integer, ServerInfo> entry : metadata.entrySet()){
				JsonObject jo2=new JsonObject()
						.add("address",((ServerInfo)entry.getValue()).getServerIP())
						.add("port",((ServerInfo)entry.getValue()).getPort())
						.add("hash",(entry.getKey()));
				ja.add(jo2);
			}
			jo.add("metadata", ja);
		}
		result=jo.toString().trim();
		return result;
	}
	
	public static ClientMessage parseFromString(String source) throws MessageParseException {
		String nKey;
		String nValue;
		StatusType nStatus;
		int nPort;
		SortedMap<Integer, ServerInfo> data;
		
		try{
			
			if(source==null || source.isEmpty()){
				throw new MessageParseException("String is null");
			}
			
			JsonObject jo=JsonObject.readFrom(source);

			nStatus=StatusType.valueOf( jo.get("statusType").asString() );
			if(jo.get("key").isNull()){
				nKey=null;
			}
			else{
				nKey=jo.get("key").asString();
			}	
			if(jo.get("value").isNull()){
				nValue=null;
			}
			else{
				nValue=jo.get("value").asString();
			}
			
			nPort=jo.get("port").asInt();
			
			if(nStatus == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
				data=new TreeMap<Integer, ServerInfo> ();
				JsonArray nested=jo.get("metadata").asArray();
				for(JsonValue ob : nested){
					JsonObject temp=ob.asObject();
					String ad=temp.get("address").asString();
					int port=temp.get("port").asInt();
					int hash=temp.get("hash").asInt();
					data.put(hash, new ServerInfo(ad,port));
				}
				return new ClientMessage(data);
			}
			return new ClientMessage(nStatus,nKey,nValue,nPort);
					
		}catch(Exception e){
			throw new MessageParseException("ClientMessage : " +e.getMessage());
		}
	}
	
	public String test(){
		String result;
		JsonObject jo=new JsonObject();
		jo.add("messageType", this.messageType.toString());
		jo.add("statusType", StatusType.SERVER_NOT_RESPONSIBLE.toString());
		jo.add("key", this.key);
		jo.add("value", this.value);
		JsonArray ja= new JsonArray();
		JsonObject jo2=new JsonObject().add("address", "120").add("port", 200).add("hash",300);
		ja.add(jo2);
		jo2=new JsonObject().add("address", "123").add("port", 250).add("hash", 200);
		ja.add(jo2);
		jo.add("metadata", ja);
		return jo.toString();
	}
	
	public static void main(String args[]){
//	//	ClientMessage cm = new ClientMessage("kostas","angelo",StatusType.SUBSCRIBE);
//		System.out.println(cm.getJson());
//		try{
//			ClientMessage lol=parseFromString(cm.getJson());
//			System.out.println(lol.getJson());
//	//			for(Map.Entry<Integer, ServerInfo> entry : lol.getMetadata().entrySet()){
//	//				System.out.println(entry.getValue().getServerIP()+" "+entry.getValue().getPort()+" "+entry.getKey());
//	//			}
//			}catch(Exception e){
//					e.printStackTrace();
//				}
//		
	}


}
