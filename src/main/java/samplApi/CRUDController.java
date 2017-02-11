package samplApi;

import java.sql.Array;
import java.util.Arrays;
import java.util.HashMap;

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
		
	@RequestMapping(value = "/{uriType}", method = RequestMethod.POST, consumes = "application/json")
	public ResponseEntity<Object> create(@RequestHeader HttpHeaders headers, @RequestBody String entity, @PathVariable String uriType) throws Exception{
		
		JSONObject object = (JSONObject) new JSONParser().parse(entity);
		
		HashMap hm = new HashMap();
		hm = (HashMap) new JSONParser().parse(object.toString());
		
		JSONObject json = new JSONObject();
		json.putAll(hm);
		
// for some time	pushJsontoRedis(object);
		
		HashMap hm1 = makeGrandHashMap(hm);
		
//		for(Object o : hm.keySet()){
//		System.out.println(o + "/"+hm.get(o) + " - "+ hm.get(o).getClass().getName());
//		}
//		
//		if(hm.containsKey("$schema"))
//			redisConnection.getJedis().set("schema^" + object.get("$schema").toString(), object.toString());
//		
//		if(hm.containsKey("_id"))
//		redisConnection.getJedis().set(object.get("_id").toString(), object.toString());
		
		JSONObject jsonZ = new JSONObject();
		jsonZ.putAll(hm1);
		
		String key = jsonZ.get("_id").toString();
		
		redisConnection.getJedis().set(key, jsonZ.toString());
			
		return new ResponseEntity<Object>(object, HttpStatus.CREATED);
	}
	
	
	
	
	
	
	
		
	public void pushJsontoRedis(JSONObject obj) throws ParseException{
		
		HashMap hm =  (HashMap) new JSONParser().parse(obj.toString());
		
		for(Object o : hm.keySet()){
			
			if(hm.get(o) instanceof JSONObject){
				JSONObject jsonObject = (JSONObject) hm.get(o);
				String keyGen = jsonObject.get("_id").toString();
					redisConnection.getJedis().set(keyGen, jsonObject.toString());
					hm.put(o, keyGen);
				
			}
			
			if(hm.get(o) instanceof JSONArray){
				
				JSONArray jsonArray = (JSONArray) hm.get(o);
				
				String[] stringArray = new String[jsonArray.size()];
				
				for(int i = 0; i < jsonArray.size(); i++){
					JSONObject jsonObject = (JSONObject) jsonArray.get(i);
					String key = jsonObject.get("_id").toString();
					stringArray[i] = key;
					redisConnection.getJedis().set(key, jsonObject.toString());
					//inspect if that thing contains more JSON
				}
				
				hm.put(o, stringArray);
				
			}
			
		}
		
		String key = obj.get("_id").toString();
		
		JSONObject json = new JSONObject();
		json.putAll(hm);
		
		redisConnection.getJedis().set(key, json.toString());
		
	}
	
	public HashMap reconstructObject(HashMap hm){
		
		for(Object o : hm.keySet()){
			
		}
		
		
		return null;
	}
	

	public HashMap makeGrandHashMap(HashMap hm) throws ParseException{
		
		for(Object o : hm.keySet()){
			
			if(hm.get(o) instanceof JSONObject){
				
				
				JSONObject jsonObject = (JSONObject) hm.get(o);
				
				HashMap hmL =  (HashMap) jsonObject;
				
				String keyGen = jsonObject.get("_id").toString();
					redisConnection.getJedis().set(keyGen, jsonObject.toString());
					
					if(!isSimpleJson(jsonObject)){
						
						makeGrandHashMap(hmL);
						
					}
					
					hm.put(o, keyGen);
				
			}
			
			if(hm.get(o) instanceof JSONArray){
				
				JSONArray jsonArray = (JSONArray) hm.get(o);
				
				String[] stringArray = new String[jsonArray.size()];
				
				for(int i = 0; i < jsonArray.size(); i++){
					JSONObject jsonObject = (JSONObject) jsonArray.get(i);
					String key = jsonObject.get("_id").toString();
					stringArray[i] = key;
					redisConnection.getJedis().set(key, jsonObject.toString());
					
					if(!isSimpleJson(jsonObject)){
						
						makeGrandHashMap((HashMap) jsonObject);
						
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
		String result = redisConnection.getJedis().get(id);
		HashMap hm = new HashMap();
		if(result != null){
			JSONObject json = (JSONObject) new JSONParser().parse(result);
			if(!json.get("_type").equals(uriType)){
				result = null;
			}
		}
		return new ResponseEntity<Object>(result == null ? null : (JSONObject) new JSONParser().parse(result),result == null ? HttpStatus.FOUND :HttpStatus.NOT_FOUND);		
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
			
			//deleting all sub Json
			
			
			
		System.out.println("Keys : " + key);
			
			
		}
	}

}
