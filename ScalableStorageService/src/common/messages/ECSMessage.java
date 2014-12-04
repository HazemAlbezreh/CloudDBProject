package common.messages;

import java.io.Serializable;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

import common.messages.KVMessage.StatusType;
import config.ServerInfo;
import consistent_hashing.Range;

public class ECSMessage implements ConfigMessage,Message,Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2046485733689892863L;
	private Range range=null;
	private ServerInfo server=null;
	private ConfigMessage.StatusType statusType ;
	private SortedMap<Integer, ServerInfo> ring=null;
	private Message.MessageType messageType=Message.MessageType.CONFIGMESSAGE;
	
	
	public ECSMessage(ConfigMessage.StatusType type){
		this.setStatus(type);
	}
	
	public ECSMessage(StatusType type,ServerInfo server,Range range){
		this.setStatus(type);
		this.setRange(range);
		this.setServerInfo(server);
	}
	
	public ECSMessage(SortedMap<Integer, ServerInfo> ring) {
		this(ConfigMessage.StatusType.UPDATE_META_DATA);
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
		return statusType;
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
	 * @param statusType the statusType to set
	 */
	public void setStatus(StatusType statusType) {
		this.statusType = statusType;
	}

	/**
	 * @param ring the ring to set
	 */
	public void setRing(SortedMap<Integer, ServerInfo> ring) {
		this.ring = ring;
	}

	@Override
	public MessageType getMessageType() {
		return this.messageType;
	}

	@Override
	public String getJson() {
		String result;
		JsonObject jo=new JsonObject();	
		String nullie=null;
		jo.add("messageType", this.messageType.toString());
		jo.add("statusType", this.statusType.toString());
		
		if(this.range != null){
			JsonObject temp= new JsonObject();
			temp.add("high",this.range.getHigh()).add("low", this.range.getLow());
			jo.add("range", temp);
		}else{
			jo.add("range", nullie);
		}
		
		if(this.server != null){
			JsonObject temp= new JsonObject();
			temp.add("address", this.server.getServerIP()).add("port", this.server.getPort());
			jo.add("server",temp);
		}else{
			jo.add("server", nullie);
		}
		
		if(this.ring != null){
			JsonArray ja= new JsonArray();
			for(Map.Entry<Integer, ServerInfo> entry : ring.entrySet()){
				JsonObject jo2=new JsonObject()
						.add("address",((ServerInfo)entry.getValue()).getServerIP())
						.add("port",((ServerInfo)entry.getValue()).getPort())
						.add("hash",(entry.getKey()));
				ja.add(jo2);
			}
			jo.add("metadata", ja);
		}else{
			jo.add("metadata", nullie);
		}
		result=jo.toString().trim();
		return result;
	}
	

	public static ECSMessage parseFromString(String source) throws MessageParseException{
		Range nRange;
		ServerInfo nServer;
		ConfigMessage.StatusType nStatus;
		SortedMap<Integer, ServerInfo> data;
		
		try{
			if(source==null || source.isEmpty()){
				throw new MessageParseException("String is null");
			}
			JsonObject jo=JsonObject.readFrom(source);
			
			nStatus=StatusType.valueOf( jo.get("statusType").asString() );
			
			if(jo.get("range").isNull()){
				nRange=null;
			}else{
				JsonObject jo2=jo.get("range").asObject();
				nRange=new Range(jo2.get("low").asInt(),jo2.get("high").asInt());
			}
			
			if(jo.get("server").isNull()){
				nServer=null;
			}else{
				JsonObject jo2=jo.get("server").asObject();
				nServer=new ServerInfo(jo2.get("address").asString(),jo2.get("port").asInt());
			}
			

			if(nStatus==ConfigMessage.StatusType.UPDATE_META_DATA){
				JsonArray nested=jo.get("metadata").asArray();
				data=new TreeMap<Integer, ServerInfo> ();

				for(JsonValue ob : nested){
					JsonObject temp=ob.asObject();
					String ad=temp.get("address").asString();
					int port=temp.get("port").asInt();
					int hash=temp.get("hash").asInt();
					data.put(hash, new ServerInfo(ad,port));
				}
				return new ECSMessage(data);
			}
			return new ECSMessage(nStatus,nServer,nRange);
					
		}catch(Exception e){
			throw new MessageParseException("ECSMessage : " +e.getMessage());
		}	
	}
}
