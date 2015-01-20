package ecs;

import common.messages.KVMessage;
import common.messages.TextMessage;

public interface ClientSocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	public void handleNewMessage(TextMessage msg);
	
	public void handleStatus(SocketStatus status);

}
