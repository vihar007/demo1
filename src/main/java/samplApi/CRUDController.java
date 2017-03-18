package samplApi;

import java.sql.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jackson.JsonObjectDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import redis.clients.jedis.Jedis;
import redisConn.RedisConnection;

@RestController
public class CRUDController {
	
	@Bean
	public RedisConnection redisConnection(){
		return new RedisConnection();
	}
	
	@Autowired
	RedisConnection redisConnection;
	
	
	@RequestMapping(value = "/schema", method = RequestMethod.POST, consumes = "application/json")
	public ResponseEntity<Object> createSchema(@RequestHeader HttpHeaders headers, @RequestBody String entity) throws Exception{
		
		JSONObject object = (JSONObject) new JSONParser().parse(entity);
		
		if(redisConnection.getJedis().get(object.get("$schema").toString()) == null){
			
			redisConnection.getJedis().set(object.get("$schema").toString(), object.toString());
			
			//hset(key, field, value);
			
			HashMap hm  = (HashMap) object;
			
			return new ResponseEntity<Object>(object,HttpStatus.CREATED);
			
		}		
	return new ResponseEntity<Object>(object, HttpStatus.BAD_REQUEST);
	}
	
	
		
	@RequestMapping(value = "/{uriType}", method = RequestMethod.POST, consumes = "application/json")
	public ResponseEntity<Object> create(@RequestHeader HttpHeaders headers, @RequestBody String entity, @PathVariable String uriType) throws Exception{
		
		String schema = redisConnection.getJedis().get(uriType);
		
		
		
		if(schema == null){
			return new ResponseEntity<Object>(null, HttpStatus.PRECONDITION_FAILED);
		} else {
			
			JSONObject schemaObject = (JSONObject) new JSONParser().parse(schema);
			
			if(!JSonValidation.validate(entity, schemaObject.toString())){
				return new ResponseEntity<Object>(null, HttpStatus.PRECONDITION_FAILED);
			}
			
		} 
		
		
		JSONObject object = (JSONObject) new JSONParser().parse(entity);
		
		
		
		HashMap hm = new HashMap();
		hm = (HashMap) new JSONParser().parse(object.toString());
		
		JSONObject json = new JSONObject();
		json.putAll(hm);
		
		String key = object.get("_id").toString();
		
		HashMap hm1 = makeGrandHashMap(hm,key);
				
		JSONObject jsonZ = new JSONObject();
		jsonZ.putAll(hm1);
		
		String key1 = jsonZ.get("_id").toString();
		
		//redisConnection.getJedis().set(key, jsonZ.toString());
		
		redisConnection.getJedis().hset(key1,jsonZ.get("_type").toString() ,jsonZ.toString());
			
		return new ResponseEntity<Object>(object, HttpStatus.CREATED);
	}
	

	
	public HashMap makeGrandHashMap(HashMap hm,String key) throws ParseException{
		
		for(Object o : hm.keySet()){
			
			if(hm.get(o) instanceof JSONObject){
				
				
				JSONObject jsonObject = (JSONObject) hm.get(o);
				
				HashMap hmL =  (HashMap) jsonObject;
				
				String keyGen = jsonObject.get("_id").toString();
					//redisConnection.getJedis().set(keyGen, jsonObject.toString());
					
					redisConnection.getJedis().hset(keyGen, jsonObject.get("_type").toString()+ ","+key, jsonObject.toString());
					
					if(!isSimpleJson(jsonObject)){
						
						makeGrandHashMap(hmL,key);
						
					}
					
					hm.put(o, keyGen);
				
			}
			
			if(hm.get(o) instanceof JSONArray){
				
				JSONArray jsonArray = (JSONArray) hm.get(o);
				
				String[] stringArray = new String[jsonArray.size()];
				
				for(int i = 0; i < jsonArray.size(); i++){
					JSONObject jsonObject = (JSONObject) jsonArray.get(i);
					String key1 = jsonObject.get("_id").toString();
					stringArray[i] = key1;
					//redisConnection.getJedis().set(key, jsonObject.toString());
					
					redisConnection.getJedis().hset(key1, jsonObject.get("_type").toString() + ","+key, jsonObject.toString());
					
					
					
					if(!isSimpleJson(jsonObject)){
						
						makeGrandHashMap((HashMap) jsonObject,key);
						
					}
				}
				
				hm.put(o, Arrays.asList(stringArray));
				
			}
			
		}				
		return hm;		
	}
		
	public boolean isSimpleJson(JSONObject o){
		HashMap hmInside = (HashMap) o;
		for(Object key : hmInside.keySet()){
			if(((hmInside.get(key) instanceof JSONObject)||(hmInside.get(key) instanceof JSONArray))) return false;
		}
		return true;		
	}
	
	@RequestMapping(value = "/{uriType}/{id}", method = RequestMethod.GET, produces = "application/json")
	public ResponseEntity<Object> get(@RequestHeader HttpHeaders headers, @PathVariable String uriType,
			@PathVariable String id) throws ParseException{
		//String result = redisConnection.getJedis().get(id);
		
		//some research
	    Map hmz = redisConnection.getJedis().hgetAll(id);
	    
	    String resultz = null;
	    String[] keyz = null;
	    
	   for(Object keys : hmz.keySet()){
		   
		   if(keys.toString().contains(uriType)){
			   resultz = hmz.get(keys).toString();
			   keyz = keys.toString().split(",");
		   } else {
			   //will punt out
		   }
		   
		   
	   }
	    
	    //end some research
		
		String result = resultz;
				//redisConnection.getJedis().hget(id, uriType+"*");
				//get(id);
		HashMap hm = new HashMap();
		
		if(result != null){
			JSONObject json = (JSONObject) new JSONParser().parse(result);
			if(!json.get("_type").equals(uriType)){
				result = null;
			}
		}
		return new ResponseEntity<Object>(result == null ? null : (JSONObject) new JSONParser().parse(result),result == null ? HttpStatus.FOUND :HttpStatus.NOT_FOUND);		
	}
	
	@RequestMapping(value="/{uriType}/{id}", method = RequestMethod.PATCH, consumes="application/json")
	public ResponseEntity<Object> patch(@RequestHeader HttpHeaders headers,@RequestBody String entity, @PathVariable String uriType,
			@PathVariable String id) throws ParseException{
		
		String result = redisConnection.getJedis().hget(id, uriType);
		
		if(result == null) return new ResponseEntity<Object>(null, HttpStatus.NOT_FOUND);
		
		HashMap changes = (HashMap) new JSONParser().parse(entity);
		
		HashMap original = (HashMap) new JSONParser().parse(result);
		
		//Note : We will only change the String Values in fields not the JSON.
		//We will deal with fields present in original map
		
		for(Object key : changes.keySet()){
			
			if(original.get(key) instanceof JSONObject || original.get(key) instanceof JSONArray 
					|| changes.get(key) instanceof JSONObject || changes.get(key) instanceof JSONArray){
				//punt out as we do not modify an array
			}
			
			if(key.toString().equals("_id") || key.toString().equals("_type")){
				//we do not modify id and types of keys
				System.out.println("we do not modify id and types of keys" + key.toString());
			}
			if(original.containsKey(key)){
				//check if JSON is not modified
				if(!original.get(key).equals(changes.get(key))){
					original.put(key, changes.get(key));
				}
				
			} else {
				//its not a valid property
				System.out.println("its not a valid property");
			}
		}
		
		
		JSONObject json = new JSONObject();
		json.putAll(original);
		
		redisConnection.getJedis().hset(id,uriType ,json.toString());
		
		return new ResponseEntity<Object>(json, HttpStatus.CREATED);
	}
	
	@RequestMapping(value="/{uriType}", method = RequestMethod.PUT, consumes="application/json")
	public ResponseEntity<Object> update(@RequestHeader HttpHeaders headers,@RequestBody String entity) throws ParseException{
		JSONObject jsonObject = new JSONObject();
		JSONParser jsonParser = new JSONParser();
		jsonObject = (JSONObject) jsonParser.parse(entity);
		String key = (String) jsonObject.get("_id");
		String result = redisConnection.getJedis().get(key);
		if(result == null){
			return new ResponseEntity<Object>(key,HttpStatus.NOT_FOUND);
		}else{
			HashMap current = (HashMap) new JSONParser().parse(result);
			HashMap update = (HashMap) jsonObject;
			HashMap result1 = updated(current,update);
			if(result1 != null){
				JSONObject newObject = new JSONObject();
				newObject.putAll(result1);
				redisConnection.getJedis().set((String) newObject.get("_id"), newObject.toString());
				return new ResponseEntity<Object>(newObject,HttpStatus.ACCEPTED);
			}else{
				return new ResponseEntity<Object>(key,HttpStatus.NOT_FOUND);
			}
		}
	}
	
	public HashMap updated(HashMap hash1, HashMap hash2){
		for(Object o : hash1.keySet()){
			if(!hash1.get(o).equals(hash2.get(o))){
				hash1.put(o, hash1.get(o));
			}else{
				return null;
			}
		}
		return hash1;
		
	}

	
	@RequestMapping(value = "/{uriType}/{id}", method = RequestMethod.DELETE, produces = "application/json")
	public ResponseEntity<Void> delete(@RequestHeader HttpHeaders headers, @PathVariable String uriType,
			@PathVariable String id) throws ParseException{
		
		
		String result = redisConnection.getJedis().get(id);
		if(result == null) {
			JSONObject json = (JSONObject) new JSONParser().parse(result);
			if(!json.get("_type").equals(uriType)){
				return new ResponseEntity<Void>(HttpStatus.NOT_FOUND);
			}		
		}
		
		
			cascadeDelete((HashMap) new JSONParser().parse(result));
		

		
	//	if(hm.containsKey("_id")) 
	//		redisConnection.getJedis().del(id);
		return new ResponseEntity<>(HttpStatus.OK);
	}
	
	static int pppp = 0;
	
	public void cascadeDelete(HashMap hm) throws ParseException {
		
		
		
		System.out.println("I am called " + ++pppp + " times");
		
		for(Object key : hm.keySet()){
			
			if(key.equals("_id")){
				redisConnection.getJedis().del(hm.get(key).toString());
			} else if(hm.get(key) instanceof JSONArray){
				JSONArray jsonArray = (JSONArray) hm.get(key);
				for(int i = 0 ; i < jsonArray.size() ; i++){
					JSONObject json = (JSONObject) new JSONParser().parse(redisConnection.getJedis().get(jsonArray.get(i).toString()));
					cascadeDelete((HashMap) json);
					System.out.println("Inside JSON");
					redisConnection.getJedis().del(jsonArray.get(i).toString());
				}	
			} else if(hm.get(key) instanceof JSONObject){
				System.out.println("Ouch I have JSON KEY" + hm.get(key).toString());
				JSONObject resJson = (JSONObject) new JSONParser().parse(hm.get(key).toString());
				redisConnection.getJedis().del(resJson.get("_id").toString());
				if(!isSimpleJson(resJson)){
					cascadeDelete((HashMap) resJson);
					System.out.println("I am not simple JSON");
				}  else {
					System.out.println("****************SIMPLE JSON*******************");
				}
			} else {
				{
					String kye = hm.get(key).toString();
					String result = redisConnection.getJedis().get(hm.get(key).toString());
					if(result != null){
						JSONObject resJson = (JSONObject) new JSONParser().parse(result);
						redisConnection.getJedis().del(resJson.get("_id").toString());
						if(!isSimpleJson(resJson)){
							cascadeDelete((HashMap) resJson);
							System.out.println("I am not simple JSON");
						}  else {
							System.out.println("****************SIMPLE JSON*******************");
						}
											
					}
				}
			}
						
		}
	}
	
	
	 @RequestMapping(value = { "/{level1}/{id1}",
		     "/{level1}/{id}/{level2}/{id2}",
		     "/{level1}/{id1}/{level2}/{id2}/{level3}/{id3}",
		     "/{level1}/{id1}/{level2}/{id2}/{level3}/{id3}/{level4}/{id4}" }, method
		     = RequestMethod.PATCH)
		     @ResponseBody
		     public String test_nesting_patch(@RequestBody JSONObject newJsonObject,
		     @PathVariable Map<String, String> pathVariables) throws ParseException {
		    //
		     if (pathVariables.containsKey("level4")) {
		      return pathVariables.get("id4");
		     } else if (pathVariables.containsKey("level3")) {
		      return pathVariables.get("id3");
		     } else if (pathVariables.containsKey("level2")) {
		     return pathVariables.get("id2");
		     } else if (pathVariables.containsKey("level1")) {
		    //
		    String key = pathVariables.get("id1");
		    //
		    // checkJsonAndMergeIfValid(newJsonObject, key);
		    
		    // return new JSONObject(jedis.hgetAll(pathVariables.get("id1")));
		    
		     } else {
		     return "";
		     }
		     return "";
		     }

}
