package common.messages;

public interface Message {

	public enum MessageType{
		KVMESSAGE,
		CONFIGMESSAGE,
		SERVERMESSAGE
	}
	
	public MessageType getMessageType();
	public String getJson();
	
}
