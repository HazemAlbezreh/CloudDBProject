package common.messages;

import java.util.SortedMap;

import config.ServerInfo;


public interface KVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		FAILURE,
    	TEXT_RESPONSE,			/* Print directly a message to the client */

		SERVER_STOPPED,         /* Server is stopped, no requests are processed */
    	SERVER_WRITE_LOCK,      /* Server locked for out, only get possible */
    	SERVER_NOT_RESPONSIBLE,  /* Request not successful, server not responsible for key */

    	SUBSCRIBE,
    	SUBSCRIBE_SUCCESS,
    	SUBSCRIBE_ERROR,
    	SUBSCRIBE_UPDATE
	}

	
	/**
	 * @return the key that is associated with this message,
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
	

	public SortedMap<Integer, ServerInfo> getMetadata();
	
}
