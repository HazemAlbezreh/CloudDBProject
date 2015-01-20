package consistent_hashing;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHash<T> {
        
  private final HashFunction hashFunction;


private final SortedMap<Integer, T> ring = new TreeMap<Integer, T>();
  
  public ConsistentHash(HashFunction hashFunction) {	 
	    this.hashFunction = hashFunction;
  }
  
  public ConsistentHash(HashFunction hashFunction,Collection<T> nodes) {	 
    this.hashFunction = hashFunction;
    for (T node : nodes) {
      add(node);
    }
  }
  
  public void add(T node) {
    ring.put(hashFunction.hash(node.toString()), node);
  }
  
  public void remove(T node) {
    ring.remove(hashFunction.hash(node.toString()));
  }
  
  public SortedMap<Integer, T> getMetaData(){
	return this.ring;
  }
  
  public HashFunction getHashFunction() {
	return hashFunction;
  }
}