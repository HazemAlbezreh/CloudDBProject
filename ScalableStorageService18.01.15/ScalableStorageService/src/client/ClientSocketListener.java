package client;

import common.messages.KVMessage;
import common.messages.TextMessage;

public interface ClientSocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	public void handleNewMessage(TextMessage msg);
	
	public void handleStatus(SocketStatus status);


	public void handleNewPostKVMessage(KVMessage msg);

	public void handleNewGetKVMessage(KVMessage msg);
}
