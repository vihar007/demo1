package samplApi;

import java.util.HashMap;
import java.util.Map;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.ResponseEntity;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

public class Consumer {
//	@Autowired
//	 RedisConnection redisConnection;
public final static String QName="Plan_Queue";
public final static String inProgressQ="Inprogress_Queue";

private Jedis jedis = null;
public Jedis getJedis() {
return jedis;
}
public void setJedis(Jedis jedis) {
	this.jedis = jedis;}
public Consumer() {
//	// TODO Auto-generated constructor stub
	jedis = new Jedis("127.0.0.1");
}


public JSONObject getJsoNForIndexer(String key) throws ParseException{
	
	
	Map hmz = jedis.hgetAll(key);
    
    String resultz = null;
    
   for(Object keys : hmz.keySet()){
		   resultz = hmz.get(keys).toString();	
		   
   }
   
   HashMap hm = new HashMap();
	hm = (HashMap) new JSONParser().parse(resultz.toString());
	
	HashMap reconstructedObject = reconstructHashMap(hm);
	
	JSONObject json = new JSONObject();
	json.putAll(reconstructedObject);
	
	return json;
	
}

public HashMap reconstructHashMap(HashMap hm) throws ParseException,JedisDataException{
	
	for(Object o : hm.keySet()){
		
		//do nothing studd
		
		if(o.toString().equals("_id") || o.toString().equals("_type")){
			//do nothing
			
			System.out.println("We do nothing for id & type");
			
			System.out.println(o.toString());
		
		} else {
			
			Object value = hm.get(o);
			
			if(value instanceof JSONArray){
				
				JSONArray jsonArray = (JSONArray) hm.get(o);
				
				System.out.println("*************Inside JSON array");
				
				
				
				
				
				
				//iterate over & call the method again
				
				for(int i = 0; i < jsonArray.size(); i++){
					String key = jsonArray.get(i).toString();
					
					System.out.println("Key : " + key);
					
					if(jedis.type(key.toString()).toString().equals("hash")){
						System.out.println("Its a hash get the document and pass it further please");
						
						
						JSONObject obj = getJSONObject(key);
						
						HashMap hm1  = (HashMap) obj;
						
						jsonArray.set(i, obj);
						
						reconstructHashMap(hm1);
						
						
					}
					
					
					
					//String key1 = jsonObject.get("_id").toString();
					//stringArray[i] = key1;
				}
				
				
			} else if(value instanceof String){
				
				//retriving Map
				
				System.out.println("***********************************************");
				
				System.out.println("Key Type " + jedis.type(value.toString()));
				
				System.out.println(o.toString());
				
				System.out.println("***********************************************");
				
				if(jedis.type(value.toString()).toString().equals("hash")){
					
					JSONObject job = getJSONObject(value.toString());
					
					HashMap hm2  = (HashMap) job;
					
					hm.put(o, job);
					
					reconstructHashMap(hm2);
					
					
				}
				
			}
			
		}
		
	}
	
	
	return hm;
}

public JSONObject getJSONObject(String key) throws ParseException{
	
	
	Map hmz = jedis.hgetAll(key);
    
    String resultz = null;
    
   for(Object keys : hmz.keySet()){
		   resultz = hmz.get(keys).toString();		   
   }
	
	
	return (JSONObject) new JSONParser().parse(resultz);
	
}





public void process (){
	RestClient client = new RestClient();
	/*
	 * Block indefinitely until there is a value in the queue
	 */
	while (true){
		System.out.println( "retrieving from the queue");
		String  answer = jedis.brpoplpush(QName, inProgressQ, 0);
		System.out.println( "found the key: "+ answer);
		//String  [] parts= answer.split("_");
		String organization = answer;
//		String object_type = parts [1];
//		String object_ID= parts [2];
		try {
			String id= organization;
		   JSONObject js= getJsoNForIndexer(id);
		    HashMap hm = new HashMap();
		    JSONParser parser = new JSONParser();
		   hm= (HashMap) parser.parse(js.toString());
			hm.remove("_id");
			hm.remove("_type");
			String obj=JSONObject.toJSONString(hm);
			String returnId = client.storeInElasticSearch(answer, obj);
			if (returnId != null)  jedis.lrem(inProgressQ, 0, returnId);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	}
public static void main(String[] args) {
	// TODO Auto-generated method stub
	Consumer task= new Consumer();
	task.process();

}
	
}


/*
 * GET _search
{
  "query": {
    "match": {"linkedPlanServices.planserviceCostShares.copay":"125"}
  }
}

GET /plan_ind/s_p/_search?q=creationDate:12-2016


GET /plan_ind/s_p/_search?q=creationDate:12-2016

GET /plan_ind/s_p/12xvxc345
 */


