package common.messages;
import java.util.SortedMap;
import config.ServerInfo;
import consistent_hashing.Range;

public interface ConfigMessage {
	
    public enum StatusType {
    	INIT,
    		INIT_SUCCESS,
    		INIT_FAILURE,
    	START,
    		START_SUCCESS,
    		START_FAILURE,
    	STOP,
    		STOP_SUCCESS,
    		STOP_FAILURE,
    	LOCK_WRITE,
    		LOCK_WRITE_SUCCESS,
    		LOCK_WRITE_FAILURE,
    	UN_LOCK_WRITE,
    		UN_LOCK_WRITE_SUCCESS,
    		UN_LOCK_WRITE_FAILURE,
    	MOVE_DATA,
    		MOVE_DATA_SUCCESS,
    		MOVE_DATA_FAILURE,
    	UPDATE_META_DATA,
    		UPDATE_META_DATA_SUCCESS,
    		UPDATE_META_DATA_FAILURE,
        SHUT_DOWN    
    }

	
    public SortedMap<Integer,ServerInfo> getRing();
    
	public Range getRange();
	
	public ServerInfo getServerInfo();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
}


