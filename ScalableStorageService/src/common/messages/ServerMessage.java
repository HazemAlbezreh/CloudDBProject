package common.messages;

import java.util.Map;

import com.eclipsesource.json.JsonObject;

public class ServerMessage implements Message{
	
	private Message.MessageType messageType=MessageType.SERVERMESSAGE;
	private Map<String,String> data=null;
	
	
	public ServerMessage(Map<String,String> d){
		this.data=d;
	}
	
	public Map<String,String> getData(){
		return this.data;
	}
	
	@Override
	public MessageType getMessageType() {
		return this.messageType;
	}

	@Override
	public String getJson() {
		String result;
		JsonObject jo=new JsonObject();
		
		result=jo.toString().trim();
		return result;
	}
	
	public static ServerMessage parseFromString(String source) throws MessageParseException {
		return null;
	}

}
