package consistent_hashing;

import java.util.SortedMap;

import config.ServerInfo;

public final class CommonFunctions {
	public static ServerInfo getSuccessorNode(Object key,SortedMap<Integer,ServerInfo> ring) {
	    if (ring.isEmpty()) {
	      return null;
	    }
	    int hash = Md5HashFunction.getInstance().hash(key);
	    if (!ring.containsKey(hash)) {
	      SortedMap<Integer, ServerInfo> tailMap = ring.tailMap(hash);
	      hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
	    }
	    return ring.get(hash);
	  }
	
	
	public static ServerInfo getPredecessorNode(Object key,SortedMap<Integer,ServerInfo> ring) {
	    if (ring.isEmpty()) {
	      return null;
	    }
	    int hash = Md5HashFunction.getInstance().hash(key);
	    if (!ring.containsKey(hash)) {
	      SortedMap<Integer, ServerInfo> headMap = ring.headMap(hash);
	      hash = headMap.isEmpty() ? ring.lastKey() : headMap.lastKey();
	    }
	    return ring.get(hash);
	  }
}
