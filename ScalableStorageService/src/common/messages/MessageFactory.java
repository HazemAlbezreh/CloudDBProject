package common.messages;

import com.eclipsesource.json.JsonObject;

import consistent_hashing.Range;

public class MessageFactory {

	public static Message parse(String source) throws MessageParseException{
		
		if(source==null || source.isEmpty()){
			throw new MessageParseException("String is null");
		}
		JsonObject jo=JsonObject.readFrom(source);
		if(jo.get("messageType").isNull()){
			throw new MessageParseException("MessageFactory : Type not found");
		}else{
			Message.MessageType type=Message.MessageType.valueOf(jo.get("messageType").asString());
			switch(type){
				case KVMESSAGE:
					return ClientMessage.parseFromString(source);
				case CONFIGMESSAGE:
					return ECSMessage.parseFromString(source);
				case SERVERMESSAGE:
					return ServerMessage.parseFromString(source);
			}
		}
		return null;
	}
}
