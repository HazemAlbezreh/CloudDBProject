package app_kvServer;

import java.io.BufferedReader;
import java.io.File;
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
	private String datasetName;
	private String replicaName;
	/**
	 * Start KV Server at given port
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 */
	
	public KVCache(String serverName, int cacheSize, String strategy, String datasetName, String replicaName) {
		counter++;
		this.cachesize = cacheSize;
		cache = new LinkedHashMap<String,MapValue>(cachesize); 
		this.strategy = strategy; 
		this.serverName = serverName; 
		this.datasetName = datasetName; 
		this.replicaName = replicaName;
		File data = new File("./" + serverName + datasetName + ".txt"); 
		File replica = new File("./" + serverName + replicaName + ".txt");
		if(data.exists() && !data.isDirectory() && counter !=1 ){
			data.delete();
			try{
				data.createNewFile(); 
			}
			catch(IOException e){
				
			}
		}
		else{
			try{
				data.createNewFile(); 
			}
			catch(IOException e){
				
			}
		}
		
		if(replica.exists() && !replica.isDirectory()){
			replica.delete();
			try{
				data.createNewFile(); 
			}
			catch(IOException e){
				
			}
		}
		else{
				try{
					replica.createNewFile();
				}
				catch(IOException e){
					
				}
			}
	}
	
	static int counter = 0;
	
	public synchronized String getDatasetName(){
		return datasetName;
	}
	
	public synchronized void setDatasetName(String name){
		datasetName = name;
	}
	
	public synchronized String getReplicaName(){
		return replicaName;
	}
	
	public synchronized void setReplicaName(String name){
		replicaName = name;
	}
	
	
	public synchronized Map<String,String> findValuesInRange(Range range, HashFunction hashfunct,String fileName){
		String line;
		HashMap<String,String> data= new HashMap<String,String>();
		boolean inRange;
		int hashvalue = 0;
		File file = new File(fileName);
		if(file.exists())
		{
			try{	
				BufferedReader br = new BufferedReader(new FileReader("./"+serverName+ fileName +".txt"));
				while ((line = br.readLine()) != null) {
					String [] str = line.split(",");
					hashvalue = hashfunct.hash(str[0]);
					inRange=range.isWithin(hashvalue);
					if(inRange){
						if(str.length>2)
							data.put(str[0], str[1]+","+str[2]);
						else
							data.put(str[0], str[1]+",");
					}
				}
				br.close();
			}
			catch(Exception e){
				
			}
		}else{
				try{
					file.createNewFile();
				} catch (IOException e) {
					
				}
			}
		return data;
	}
	
	
	public synchronized String processPutRequest(String key, String value, ArrayList<String> subscriptions ,String fileName){
		String updateResult = updateDatasetEntry(key, value,subscriptions,fileName);
		PrintWriter pr = null;
		if(updateResult.equals("UPDATE_NOT_PERFORMD")){
			try {
					pr = new PrintWriter(new FileWriter("./"+serverName+ fileName +".txt",true));
					pr.println(key + "," + value + ",");
					pr.close();
					addCacheEntry(key, value);
					return "PUT_SUCCESS";
			}
			catch(IOException e) {
				//e.printStackTrace();
				return "PUT_ERROR";
			}
		}
		else{
			//addCacheEntry(key, value);
			return "PUT_UPDATE";
		}
	}
	 
		
	public synchronized String processMassPutRequest(Map<String,String> values,String fileName){
		Set s = values.entrySet();
		MapValue mp = null;
		Iterator itr = s.iterator();
		PrintWriter pr;
		String key,value;
		int cachCount = 0;
			try {
				pr = new PrintWriter(new FileWriter("./"+serverName+ fileName +".txt",true));
				itr= s.iterator();
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
			catch(Exception e) {
				//e.printStackTrace();
				return "PUT_ERROR";
			}
		 return "PUT_SUCCESS";
	}
	
	
	public synchronized String processGetRequest(String key,String fileName){
		//check the cache first
		String response = checkGetHitOrMiss(key);
		if(response != null )
			return response;
		//if not found in cache search in the file
		else{
			String line;
			try{
				BufferedReader br = new BufferedReader(new FileReader("./"+serverName+ fileName +".txt"));
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

	
	/*
	public synchronized String processGetRequest(String key){
		//check the cache first
		String response = checkGetHitOrMiss(key);
		if(response != null )
			return response;
		//if not found in cache search in the file
		else{
			String line;
			try{
				BufferedReader br = new BufferedReader(new FileReader("./"+serverName+ datasetName +".txt"));
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
	*/
	
	
	public synchronized ArrayList<String> parseClientsAdresses(String input){
		ArrayList<String> parsedList = new ArrayList<String>();
		String []str = null;
		if(!input.isEmpty()){
			str = input.split("-");
			for(int i =0 ;i<str.length;i++){
				parsedList.add(str[i]);
			}
		}
		return parsedList;
	}
	
	public synchronized String updateDatasetEntry(String key,String newValue, ArrayList<String> subscriptions, String fileName){
		StringBuilder sbld = new StringBuilder();
		String newline = System.getProperty("line.separator");
		String updateResult = "";
		
		try{
			BufferedReader br = new BufferedReader(new FileReader("./"+serverName+fileName+".txt"));
			String line = "";
		//	boolean emptyFile= true;
			while ((line = br.readLine()) != null) {
				//emptyFile=false;
				String [] str = line.split(",");
				if(str[0].equals(key)){
					updateResult = "PUT_UPDATE";
					if(str.length>2){
						sbld.append(str[0] + "," + newValue + "," + str[2] + newline);
						subscriptions = parseClientsAdresses(str[2]);
					}
					else
						sbld.append(str[0] + "," + newValue + "," + newline);
				}
				else
					sbld.append(line + newline);
			}
			br.close();
	//		if (emptyFile)
		}
		catch(IOException e){
		//	e.printStackTrace();
			
		}
		
		if(updateResult.equals("PUT_UPDATE")){
			try{
				PrintWriter pr = new PrintWriter(new FileWriter("./"+serverName+fileName+".txt"));
				pr.print(sbld);
				pr.close();
				if(!cache.containsKey(key)){
					addCacheEntry(key,newValue);
				}
			}
			catch (IOException e) {
				//e.printStackTrace();
			}
		}
		else
			updateResult = "UPDATE_NOT_PERFORMD";
		return updateResult;
	}
	

	public synchronized String deleteEntry(String key, String fileName, ArrayList<String> subscriptions){
		StringBuilder sbld = new StringBuilder();
		String newline = System.getProperty("line.separator");
		String deleteResult = "";
		BufferedReader br = null;
		PrintWriter pr = null;
		if(cache.containsKey(key))
			cache.remove(key);
		try{
				br = new BufferedReader(new FileReader("./"+serverName+fileName+".txt"));
			String line = "";
			while ((line = br.readLine()) != null) {
				String [] str = line.split(",");
				if(str[0].equals(key)){
					deleteResult = "DELETE_SUCCESS";
					if(str.length>2)
						subscriptions = parseClientsAdresses(str[2]);
					continue;
				}
				else
					sbld.append(line + newline);
			}
			br.close();
		}
		catch(IOException e){
			//e.printStackTrace();
		}
		
		if(deleteResult.equals("DELETE_SUCCESS")){
			try{
				pr = new PrintWriter(new FileWriter("./"+serverName+fileName+".txt"));
				pr.print(sbld);
				pr.close();
			}
			catch (IOException e) {
				
			}
		}
		else
			deleteResult = "DELETE_ERROR";
		
		return deleteResult;
	}
	
	
	public synchronized void deleteAllData(String fileName){
		PrintWriter pr = null;
		try{
			pr = new PrintWriter(new FileWriter("./"+serverName+ fileName+".txt"));
			pr.print("");
			pr.close();
		}
		catch(IOException e){
			
		}
	}
	
	public synchronized String deleteDatasetEntry(ArrayList<String> keys,String fileName){
		StringBuilder sbld = new StringBuilder();
		String newline = System.getProperty("line.separator");
		String deleteResult = "";
		String line = "";
		BufferedReader br = null;
		PrintWriter pr = null;
		try{
			br = new BufferedReader(new FileReader("./"+serverName+fileName+".txt"));
			for(String key:keys){
				if(cache.containsKey(key))
					cache.remove(key);
			}
				while ((line = br.readLine()) != null) {
					String [] str = line.split(",");
						if(keys.contains(str[0])){
								deleteResult = "DELETE_SUCCESS";
								keys.remove(str[0]);
							}
						else
							sbld.append(line + newline);
				}
				br.close();
		}
		catch(IOException e){
		//	e.printStackTrace();
		}
		
		if(deleteResult.equals("DELETE_SUCCESS")){
			try{
				pr = new PrintWriter(new FileWriter("./"+serverName+ fileName +".txt"));
				pr.print(sbld);
				pr.close();
			} 
			catch (IOException e) {
			//	e.printStackTrace();
			
			}
		}
		else
			deleteResult = "DELETE_ERROR";
		
		return deleteResult;
	}
	
	
	public synchronized String formatArrayList(ArrayList<String> lst){
		String result = "";
		for(int i=0 ; i<lst.size() ; i++){
			if(i+1 ==lst.size())
				result += lst.get(i);
			else
				result += lst.get(i)+"-";
		}
		return result;
	}
	
	//Subscribe Code.
	
	public synchronized String subscribe(String key, String ip, String port,String fileName){
			StringBuilder sbld = new StringBuilder();
			String newline = System.getProperty("line.separator");
			String updateResult = "";
			String value = null;
			try{
				BufferedReader br = new BufferedReader(new FileReader("./"+serverName+fileName+".txt"));
				String line = "";
			//	boolean emptyFile= true;
				while ((line = br.readLine()) != null) {
					//emptyFile=false;
					String [] str = line.split(",");
					
					if(str[0].equals(key)){
						if(str.length>2){
							if(str[2].contains(ip+":"+port)){
								updateResult = "IP_ADDRESS_ALREADY_EXIST";
								value = str[1];
								break;
							}
							else{
								updateResult = "SUBSCRIBTION_SUCCESS";
								value = str[1];
								sbld.append(str[0] + "," + str[1] + "," + str[2] + "-" + ip + ":" + port + newline);
							}
						}
						else{
							updateResult = "SUBSCRIBTION_SUCCESS";
							value = str[1];
							sbld.append(str[0] + "," + str[1] + "," + ip + ":"+port + newline);
						}
					}
					else
						sbld.append(line + newline);
				}
				br.close();
		//		if (emptyFile)
			}
			catch(IOException e){
				updateResult = "ERROR_HAPPEND";
			//	e.printStackTrace();
				
			}
			if(updateResult.equals("SUBSCRIBTION_SUCCESS")){
				try{
					PrintWriter pr = new PrintWriter(new FileWriter("./"+serverName+fileName+".txt"));
					pr.print(sbld);
					pr.close();
					if(!cache.containsKey(key)){
						addCacheEntry(key,value);
					}
				}
				catch (IOException e) {
					updateResult = "ERROR_HAPPEND";
					//e.printStackTrace();
				}
			}
			else if(!updateResult.equals("IP_ADDRESS_ALREADY_EXIST"))
				updateResult = "KEY_NOT_FOUND";
			
			return updateResult;
	}
	
	
	public synchronized String unSubscribe(String key, String ip, String port,String fileName){
		StringBuilder sbld = new StringBuilder();
		String newline = System.getProperty("line.separator");
		String updateResult = "";
		String value = null;
		try{
			BufferedReader br = new BufferedReader(new FileReader("./"+serverName+fileName+".txt"));
			String line = "";
		//	boolean emptyFile= true;
			while ((line = br.readLine()) != null){
				//emptyFile=false;
				String [] str = line.split(",");
				if(str[0].equals(key)){
					if(str.length>2){
						ArrayList<String>temp = parseClientsAdresses(str[2]);
						if(str[2].contains(ip+":"+port)){
							updateResult = "UNSUBSCRIBTION_SUCCESS";
							temp.remove(ip+":"+port);
							String addresses = formatArrayList(temp);
							sbld.append(str[0] + "," + str[1] + "," + addresses + newline);
						}
						else{
							updateResult = "IP_ADDRESS_NOT_EXIST";
							break;
						}
					}
					else{
						updateResult = "IP_ADDRESS_NOT_EXIST";
						break;
					}
				}
				else
					sbld.append(line + newline);
			}
			br.close();
	//		if (emptyFile)
		}
		catch(IOException e){
			updateResult = "ERROR_HAPPEND";
		//	e.printStackTrace();
			
		}
		if(updateResult.equals("UNSUBSCRIBTION_SUCCESS")){
			try{
				PrintWriter pr = new PrintWriter(new FileWriter("./"+serverName+fileName+".txt"));
				pr.print(sbld);
				pr.close();
				if(!cache.containsKey(key)){
					addCacheEntry(key,value);
				}
			}
			catch (IOException e) {
				updateResult = "ERROR_HAPPEND";
				//e.printStackTrace();
			}
		}
		else if(!updateResult.equals("IP_ADDRESS_NOT_EXIST"))
			updateResult = "KEY_NOT_FOUND";
		
		return updateResult;
}
	
	
	
	//cache stuff
	
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
