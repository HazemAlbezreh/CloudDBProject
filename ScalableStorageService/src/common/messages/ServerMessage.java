package common.messages;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import app_kvServer.ServerStatus;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

import common.messages.ConfigMessage.StatusType;
import common.messages.ECSMessage.MoveCaseType;
import config.ServerInfo;
import consistent_hashing.Range;

public class ServerMessage implements Message{
	
	public enum StatusType{
		DATA_TRANSFER,								//Move data across servers
			DATA_TRANSFER_SUCCESS,
			DATA_TRANSFER_FAILED,
		INIT_REPLICA,
		UPDATE_REPLICA,
		DELETEFROM_REPLICA,
		UPDATE_RANGE_REPLICA,
		REPLICA_FAILURE,
		DELETEFROM_REPLICA_FAILED,
		DELETEFROM_REPLICA_SUCCESS,
	}
	
	private Message.MessageType messageType=MessageType.SERVERMESSAGE;
	private StatusType statusType;
	private Map<String,String> data=null;
	private Range range=null;
	
	private ECSMessage.MoveCaseType movecase=null;
	
	public String getMoveCase(){
		return movecase.toString();
	}
	
	public ServerMessage(Map<String,String> d){
		this.data=d;
		this.statusType=ServerMessage.StatusType.DATA_TRANSFER;
	}
	
	public ServerMessage(Map<String,String> d, ECSMessage.MoveCaseType caseType){
		this.data=d;
		this.statusType=ServerMessage.StatusType.DATA_TRANSFER;
		this.movecase = caseType;
	}
	
	public ServerMessage(ServerMessage.StatusType type,Map<String,String> d){
		this.data=d;
		this.statusType=type;
	}
	
	public ServerMessage(ServerMessage.StatusType type){
		this.statusType=type;
	}
	
	public ServerMessage(ServerMessage.StatusType type,Range r){
		this.statusType=type;
		this.range=r;
	}
	/////////////////////////////////////////////
	public ServerMessage(ServerMessage.StatusType type,Map<String,String> d, ECSMessage.MoveCaseType move){
		this.data=d;
		this.statusType=type;
		this.movecase=move;
	}
	
	/////////////////////////////////////////////
	
	public Range getRange(){
		return range;
	}
	
	public Map<String,String> getData(){
		return this.data;
	}
	
	@Override
	public MessageType getMessageType() {
		return this.messageType;
	}
	
	public StatusType getStatus(){
		return this.statusType;
	}

	@Override
	public String getJson() {
		String result;
		JsonObject jo=new JsonObject();
		String nullie=null;
		
		jo.add("messageType", this.messageType.toString());
		jo.add("statusType", this.statusType.toString());
		
		if(this.movecase!=null){
			jo.add("moveType", this.movecase.toString());			
		}else{
			jo.add("moveType", nullie);
		}
		
		if(this.data != null){
			JsonArray ja= new JsonArray();
			for(Map.Entry<String, String> entry : data.entrySet()){
				JsonObject jo2=new JsonObject()
						.add("key",(String)entry.getKey())
						.add("value",(String)entry.getValue());
				ja.add(jo2);
			}
			jo.add("data", ja);
		}else{
			jo.add("data", nullie);
		}
		
		result=jo.toString().trim();
		return result;
	}
	
	public static ServerMessage parseFromString(String source) throws MessageParseException {
		
		ServerMessage.StatusType nStatus;
		ECSMessage.MoveCaseType nMove=null;
		Map<String,String> nData=null;
		
		try{
			if(source==null || source.isEmpty()){
				throw new MessageParseException("String is null");
			}
			JsonObject jo=JsonObject.readFrom(source);
			
			nStatus=StatusType.valueOf( jo.get("statusType").asString() );
			
			if(jo.get("moveType").isNull()){
				nMove=null;
			}else{
				nMove=MoveCaseType.valueOf( jo.get("moveType").asString() );
			}
			
			if(jo.get("data").isNull()){
				nData=new HashMap<String,String>();
			}else{
				JsonArray nested=jo.get("data").asArray();
				nData=new HashMap<String,String>();
				for(JsonValue ob : nested){
					JsonObject temp=ob.asObject();
					String key=temp.get("key").asString();
					String value=temp.get("value").asString();
					nData.put(key,value);
				}
			}
			return new ServerMessage(nStatus,nData,nMove);
		}
		catch(Exception e){
			throw new MessageParseException("ServerMessage : " +e.getMessage());
		}	
	}
	
	
	public static void main(String args[]){
		ServerMessage sm=new ServerMessage(new TreeMap<String,String>());
		System.out.println(sm.getJson());
		try{
		ServerMessage sm2=(ServerMessage)MessageFactory.parse(sm.getJson());
		String s="";
		}catch(Exception e){System.out.println(e.getMessage());}
	}
	
	
}
