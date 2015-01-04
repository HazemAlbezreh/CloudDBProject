package consistent_hashing;

import java.util.SortedMap;

import config.ServerInfo;

public final class CommonFunctions {
	public static ServerInfo getSuccessorNode(Object key,SortedMap<Integer,ServerInfo> ring) {
	    if (ring.isEmpty()) {
	      return null;
	    }
	    int hash = Md5HashFunction.getInstance().hash(key);
	    int nodeHash;
	    if (!ring.containsKey(hash)) {
	      SortedMap<Integer, ServerInfo> tailMap = ring.tailMap(hash);
	      nodeHash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
	    }else {
	    	SortedMap<Integer, ServerInfo> tailMap = ring.tailMap(hash+1);
	    	nodeHash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
	    }
	    return ring.get(nodeHash);
	    
	  }
	
	
	public static ServerInfo getPredecessorNode(Object key,SortedMap<Integer,ServerInfo> ring) {
	    if (ring.isEmpty()) {
	      return null;
	    }
	    int hash = Md5HashFunction.getInstance().hash(key);
	    int nodeHash;
	    if (!ring.containsKey(hash)) {
	      SortedMap<Integer, ServerInfo> headMap = ring.headMap(hash);
	      nodeHash = headMap.isEmpty() ? ring.lastKey() : headMap.lastKey();
	    }else {
	    	SortedMap<Integer, ServerInfo> headMap = ring.headMap(hash-1);
		      nodeHash = headMap.isEmpty() ? ring.lastKey() : headMap.lastKey();
	    }
	    return ring.get(nodeHash);
	  }
	
	
	public static ServerInfo getSecondSuccessorNode(Object key,SortedMap<Integer,ServerInfo> ring) {
	    if (ring.isEmpty()) {
	      return null;
	    }
	    int hash = Md5HashFunction.getInstance().hash(key);
	    int nodeHash;
	    if (!ring.containsKey(hash)) {
	      SortedMap<Integer, ServerInfo> tailMap = ring.tailMap(hash);
	      tailMap = ring.tailMap(tailMap.firstKey()+1);
	      nodeHash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
	    }else {
	    	SortedMap<Integer, ServerInfo> tailMap = ring.tailMap(hash+1);
	    	tailMap = ring.tailMap(tailMap.firstKey()+1);
	    	nodeHash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
	    }
	    return ring.get(nodeHash);
	  }
	
	//TO-DO this probably does not work in case of a key being equal to the value 
}