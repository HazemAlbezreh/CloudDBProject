package common.messages;
import java.util.SortedMap;
import config.ServerInfo;
import consistent_hashing.Range;

public interface ConfigMessage {
	
    public enum StatusType {
    	INIT,
    	START,
    	START_SUCCESS,
    	STOP,
    	STOP_SUCCESS,
    	SHUT_DOWN,    
    	lOCK_WRITE,
    	lOCK_WRITE_SUCCESS,
    	UN_lOCK_WRITE,
    	UN_lOCK_WRITE_SUCCESS,
    	MOVE_DATA,
    	MOVE_DATA_SUCCESS,
    	UPDATE_META_DATA,
    	UPDATE_META_DATA_SUCCESS,
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


