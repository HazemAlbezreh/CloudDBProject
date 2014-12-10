package app_kvServer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import consistent_hashing.HashFunction;
import consistent_hashing.Range;


 
public class KVCache  {
	
	
	private LinkedHashMap<String, MapValue> cache;
	private int cachesize;
	private String strategy;
	private String serverName;
	/**
	 * Start KV Server at given port
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 */
	
	public KVCache(String serverName, int cacheSize, String strategy) {
		this.cachesize = cacheSize;
		cache = new LinkedHashMap<String,MapValue>(cachesize);
		this.strategy = strategy;
		this.serverName = serverName;
	}
	
	public synchronized Map<String,String> calculateRange(int low, int high, HashFunction hashfunct){
		String line;
		HashMap<String,String> outOfRange= new HashMap<String,String>();
		int hashvalue = 0;
		try{
			BufferedReader br = new BufferedReader(new FileReader("./"+serverName+"dataset.txt"));
			while ((line = br.readLine()) != null) {
				String [] str = line.split(",");
				hashvalue = hashfunct.hash(str[0]);
				
				if(!(hashvalue >= low && hashvalue < high)){
					outOfRange.put(str[0], str[1]);
				}
			}
			br.close();
			return outOfRange;
		}
		catch(Exception e){
			return null;
		}
	}

	
	public synchronized Map<String,String> calculateRange(Range range, HashFunction hashfunct){
		String line;
		HashMap<String,String> outOfRange= new HashMap<String,String>();
		boolean inRange;
		int hashvalue = 0;
		try{
			BufferedReader br = new BufferedReader(new FileReader("./"+serverName+"dataset.txt"));
			while ((line = br.readLine()) != null) {
				String [] str = line.split(",");
				hashvalue = hashfunct.hash(str[0]);
				inRange=range.isWithin(hashvalue);
				if(!inRange){
					outOfRange.put(str[0], str[1]);
				}
			}
			br.close();
			return outOfRange;
		}
		catch(Exception e){
			return null;
		}
	}
	
	public synchronized String processPutRequest(Map<String,String> values){
		String updateResult = "";
		Set s = values.entrySet();
		MapValue mp = null;
		Iterator itr = s.iterator();
		String key,value ;
		if(values.size() == 1){
			Map.Entry ent = (Map.Entry)itr.next();
			key = ((String)ent.getKey());
			value = ((String)ent.getValue());
			updateResult = updateDatasetEntry(key, value);
		}
		if(updateResult.equals("UPDATE_NOT_PERFORMD")){
		int cachCount = 0;
			try {
				PrintWriter pr = new PrintWriter(new FileWriter("./"+serverName+"dataset.txt",true));
				while(itr.hasNext()){
					Map.Entry ent = (Map.Entry)itr.next();
					key = ((String)ent.getKey());
					value = ((String)ent.getValue());
					pr.println( key+ "," +value );
					if(cachCount< cachesize){
						addCacheEntry(key, value);
						cachCount++;
					}
				}
				pr.close();
			}
			catch(IOException e) {
				e.printStackTrace();
				return "PUT_ERROR";
			}
		 return "PUT_SUCCESS";
		}
		else{
			//addCacheEntry(key, value);
			return "PUT_UPDATE";
		}
	}
	
	
	public synchronized String processGetRequest(String key){
		//check the cache first
		String response = checkGetHitOrMiss(key);
		if(response != null )
			return response;
		//if not found in cache search in the file
		else{
			String line;
			try{
				BufferedReader br = new BufferedReader(new FileReader("./"+serverName+"dataset.txt"));
				while ((line = br.readLine()) != null) {
					String [] str = line.split(",");
					if(str[0].equals(key)){
						br.close();
						addCacheEntry(str[0], str[1]);
						return str[1];
					}
				}
				br.close();
				return null;
			}
			catch(Exception e){
				return null;
			}
		}
	}
	
	
	public synchronized String updateDatasetEntry(String key,String newValue){
		StringBuilder sbld = new StringBuilder();
		String newline = System.getProperty("line.separator");
		String updateResult = "";
		if(cache.containsKey(key)){
			MapValue entry = cache.get(key);
			entry.setValue(newValue);
			cache.put(key, entry);
				updateCache(key);
		}
		
		try{
			BufferedReader br = new BufferedReader(new FileReader("./"+serverName+"dataset.txt"));
			String line = "";
			while ((line = br.readLine()) != null) {
				String [] str = line.split(",");
				if(str[0].equals(key)){
					updateResult = "PUT_UPDATE";
					sbld.append(str[0] + "," + newValue + newline);
				}
				else
					sbld.append(line + newline);
			}
		}
		catch(IOException e){
			e.printStackTrace();
		}
		
		if(updateResult.equals("PUT_UPDATE")){
			try{
				PrintWriter pr = new PrintWriter(new FileWriter("./"+serverName+"dataset.txt"));
				pr.print(sbld);
				pr.close();
				if(!cache.containsKey(key)){

				addCacheEntry(key,newValue);}
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		else
			updateResult = "UPDATE_NOT_PERFORMD";
		return updateResult;
	}
	
	public synchronized String deleteDatasetEntry(ArrayList<String> keys){
		StringBuilder sbld = new StringBuilder();
		String newline = System.getProperty("line.separator");
		String deleteResult = "";
		String line = "";
		try{
			BufferedReader br = new BufferedReader(new FileReader("./"+serverName+"dataset.txt"));
			for(String key:keys){
				if(cache.containsKey(key))
					cache.remove(key);
			}
				while ((line = br.readLine()) != null) {
					String [] str = line.split(",");
					for(String key:keys){
						if(str[0].equals(key)){
							deleteResult = "DELETE_SUCCESS";
							keys.remove(key);
							break;
						}
						else
							sbld.append(line + newline);
					}
				}
		}
		catch(IOException e){
			e.printStackTrace();
		}
		
		if(deleteResult.equals("DELETE_SUCCESS")){
			try{
				PrintWriter pr = new PrintWriter(new FileWriter("./"+serverName+"dataset.txt"));
				pr.print(sbld);
				pr.close();
			} 
			catch (IOException e) {
				e.printStackTrace();
			
			}
		}
		else
			deleteResult = "DELETE_ERROR";
		
		return deleteResult;
	}
	
	
	
	
	
/*	public String processPutRequest(String key, String value){
		String updateResult = updateDatasetEntry(key, value);
		if(updateResult.equals("UPDATE_NOT_PERFORMD")){
			try {
				PrintWriter pr = new PrintWriter(new FileWriter("./"+serverName+"dataset.txt",true));
				pr.println(key + "," + value);
				pr.close();
				addCacheEntry(key, value);
				return "PUT_SUCCESS";
			} 
			catch(IOException e) {
				e.printStackTrace();
				return "PUT_ERROR";
			}
		}
		else{
			//addCacheEntry(key, value);
			return "PUT_UPDATE";
		}
	}*/
	
	public synchronized String checkGetHitOrMiss(String key){
		String val = "";
		if(cache.containsKey(key)){
			val = cache.get(key).getValue();
			if(strategy.equals("LRU") || strategy.equals("LFU"))
				updateCache(key);
			return val;			
		}
		else
			return null;
	}

	public synchronized void updateCache(String key){
		switch(strategy){
			case "FIFO":{
				break;
			}
			case "LFU":{
				MapValue mp = cache.get(key);
				mp.setCounter(mp.getCounter() +1);
				cache.put(key, mp);
				break;
			}
			case "LRU":{
				Set s = cache.entrySet();
				MapValue mp = null;
				Iterator itr = s.iterator();
				while(itr.hasNext()){
					Map.Entry ent = (Map.Entry)itr.next();
					if(((String)ent.getKey()).equals(key)){
						mp = ((MapValue)ent.getValue());
						mp.setCounter(cache.size()-1);
						cache.put(key,mp);
					}
					else{
						mp = ((MapValue)ent.getValue());
						mp.setCounter(mp.getCounter()-1);
						cache.put((String) ent.getKey(),mp);
					}
				}
				break;
			}
		}
	}

	
	public synchronized String findMinScore(String key){
		Set st = cache.entrySet();
		Iterator itr = st.iterator();
		MapValue mapval = new MapValue();
		String tempKey = "";
		int tempCounter=0;
		if(itr.hasNext()){
			tempCounter = ((Map.Entry<String,MapValue>)cache.entrySet().iterator().next()).getValue().getCounter();
			tempKey = ((Map.Entry<String,MapValue>)cache.entrySet().iterator().next()).getKey();
		}
		while(itr.hasNext()){
			Map.Entry ent = (Map.Entry)itr.next();
			mapval = (MapValue)ent.getValue();
			if(mapval.getCounter() < tempCounter){
				tempCounter = mapval.getCounter();
				tempKey = (String)ent.getKey();
			}
		}
		return tempKey;
	}

	
	public synchronized void addCacheEntry(String key, String value){
		MapValue mp = new MapValue();
		switch(strategy){
			case "FIFO":{
				if(cache.size() == cachesize){
					Map.Entry<String,MapValue> firstEntry = (Map.Entry<String,MapValue>)cache.entrySet().iterator().next();
					cache.remove(firstEntry.getKey());
					mp.setValue(value);
					cache.put(key, mp);
				}
				else{
					mp.setValue(value);
					cache.put(key, mp);
				}
				break;
			}
			case "LFU":{
				
				if(cache.size() == cachesize){
					String tempKey = findMinScore(key);
					cache.remove(tempKey);
					mp.setValue(value);
					mp.setCounter(1);
					cache.put(key, mp);
				}
				else{
					mp.setValue(value);
					mp.setCounter(1);
					cache.put(key, mp);
				}			
				break;
			}
			case "LRU":{
				if(cache.size() == cachesize){
					String tempKey = findMinScore(key);
					cache.remove(tempKey);
					Set s = cache.entrySet();
					Iterator itr = s.iterator();
					while(itr.hasNext()){
						Map.Entry ent = (Map.Entry)itr.next();
						mp = (MapValue)ent.getValue();
						mp.setCounter(mp.getCounter()-1);
						cache.put((String)ent.getKey(),mp);
						}
					mp = new MapValue(value, cachesize -1);
					cache.put(key, mp);
				}
				else{
					Set s = cache.entrySet();
					Iterator itr = s.iterator();
					while(itr.hasNext()){
						Map.Entry ent = (Map.Entry)itr.next();
						mp = (MapValue)ent.getValue();
						mp.setCounter(mp.getCounter()-1);
						cache.put((String)ent.getKey(),mp);
						}
					mp = new MapValue(value, cachesize -1);
					cache.put(key, mp);
					}
				break;
			}
		}
	}
	
	public synchronized LinkedHashMap<String, MapValue> getCache(){
		return this.cache;
	}
}
