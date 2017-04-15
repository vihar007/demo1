package samplApi;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redisConn.RedisConnection;

@RestController
public class CRUDController {
	
	@Bean
	public RedisConnection redisConnection(){
		return new RedisConnection();
	}
	
	@Autowired
	RedisConnection redisConnection;
	
	
	private String userID = "";
	
	
	
	@RequestMapping(value="/createToken",method = RequestMethod.POST, consumes="application/json")

	public ResponseEntity<Object> generateEncryption(@RequestHeader HttpHeaders headers, @RequestBody String entity) throws Exception{

	JSONObject jsonObject = (JSONObject) new JSONParser().parse(entity);

	//String role = (String) jsonObject.get("_role");

	String userAuthontication = (String) jsonObject.get("_id");

	userID = userAuthontication;

	String authorizationHeader = Encrypter.encrypt(userAuthontication);


	return new ResponseEntity<Object>(authorizationHeader, HttpStatus.CREATED);

	}
	
	
	
	
	@RequestMapping(value = "/schema", method = RequestMethod.POST, consumes = "application/json")
	public ResponseEntity<Object> createSchema(@RequestHeader HttpHeaders headers, @RequestBody String entity) throws Exception{
		
		if(!isAuthorized(headers)) return new ResponseEntity<Object>(HttpStatus.UNAUTHORIZED);
		
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
		
		if(!isAuthorized(headers)) return new ResponseEntity<Object>(HttpStatus.UNAUTHORIZED);
		
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
		
		redisConnection.getJedis().hset(key1,jsonZ.get("_type").toString()+","+key1 ,jsonZ.toString());
			
		return new ResponseEntity<Object>(object,generateNewEtag(jsonZ) ,HttpStatus.CREATED);
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
					
					//iterate over & call the method again
					
					for(int i = 0; i < jsonArray.size(); i++){
						String key = jsonArray.get(i).toString();
						
						System.out.println("Key : " + key);
						
						if(redisConnection.getJedis().type(key.toString()).toString().equals("hash")){
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
					
					System.out.println("Key Type " + redisConnection.getJedis().type(value.toString()));
					
					System.out.println(o.toString());
					
					System.out.println("***********************************************");
					
					if(redisConnection.getJedis().type(value.toString()).toString().equals("hash")){
						
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
	
	
	//helper methods
	
	public String etagGeneration(JSONObject result) throws UnsupportedEncodingException, NoSuchAlgorithmException{

		MessageDigest messageDigest = MessageDigest.getInstance("MD5");

		byte[] bytesOfMessage = result.toJSONString().getBytes("UTF-8");

		byte[] thedigest = messageDigest.digest(bytesOfMessage);

		return	thedigest.toString();


		}


		public boolean isAuthorized(HttpHeaders headers){


		String token = headers.getFirst("Authorization");

		if(token == null){

		return false;

		}

		String decrypted;

		try {

			decrypted = Encrypter.decrypt(token);
			
		} catch (Exception e) {

		// TODO Auto-generated catch block

		e.printStackTrace();

		return false;

		}

		if(decrypted.equals(userID)){

		return true;

		}




		return false;

		}

		public boolean deleteEtag(String key){


		redisConnection.getJedis().hdel(key+"ETAG", "used");

		redisConnection.getJedis().hdel(key+"ETAG", "new");


		return true;

		}


		public MultiValueMap<String, String> generateNewEtag(JSONObject jsonObject) throws UnsupportedEncodingException, NoSuchAlgorithmException, ParseException{

		String etag = etagGeneration(jsonObject);

		redisConnection.getJedis().hset(getParent(jsonObject.get("_id").toString())+"ETAG", "new", etag);

		MultiValueMap<String, String> params = new LinkedMultiValueMap<String, String>();

		    params.set("Etag", etag);

		return params;

		}
	
	public JSONObject getJSONObject(String key) throws ParseException{
		
		
		Map hmz = redisConnection.getJedis().hgetAll(key);
	    
	    String resultz = null;
	    
	   for(Object keys : hmz.keySet()){
			   resultz = hmz.get(keys).toString();		   
	   }
		
		
		return (JSONObject) new JSONParser().parse(resultz);
		
	}
	
	
	public String getParent(String id) throws ParseException{
		
		
	    Map hmz = redisConnection.getJedis().hgetAll(id);

	    String[] keyz = null;
	    
	   for(Object keys : hmz.keySet())
		   keyz = keys.toString().split(",");

		System.out.println(id);
		
		return keyz[1];
		
	}
	

	
	public HashMap makeGrandHashMap(HashMap hm,String key) throws ParseException{
		
		for(Object o : hm.keySet()){
			
			if(hm.get(o) instanceof JSONObject){
				
				
				JSONObject jsonObject = (JSONObject) hm.get(o);
				
				HashMap hmL =  (HashMap) jsonObject;
				
				String keyGen = jsonObject.get("_id").toString();
					//redisConnection.getJedis().set(keyGen, jsonObject.toString());
					
					
					if(!isSimpleJson(jsonObject)){
						
						makeGrandHashMap(hmL,keyGen);
						
					}
					
					redisConnection.getJedis().hset(keyGen, jsonObject.get("_type").toString()+ ","+key, jsonObject.toString());
					
					
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
					
					
					
					
					if(!isSimpleJson(jsonObject)){
						
						makeGrandHashMap((HashMap) jsonObject,key1);
						
					}
					
					redisConnection.getJedis().hset(key1, jsonObject.get("_type").toString() + ","+key, jsonObject.toString());
					
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
	
	
	public boolean isUsedEtag(String id,String etag){
		
		Map<String, String> map = redisConnection.getJedis().hgetAll(id+"ETAG");
		
		if(map.containsValue(etag)){
			if(map.containsKey("used")) {
				return true;
			}
		}
		
		return false;
		
	}
	
	
	
	@RequestMapping(value = "/{uriType}/{id}", method = RequestMethod.GET, produces = "application/json")
	public ResponseEntity<Object> get(@RequestHeader HttpHeaders headers, @PathVariable String uriType,
			@PathVariable String id) throws ParseException{
		//String result = redisConnection.getJedis().get(id);
		
		if(!isAuthorized(headers)) return new ResponseEntity<Object>(HttpStatus.UNAUTHORIZED);
		
		String etag = headers.getFirst("If-None-Match");
		
		
		if(etag != null && isUsedEtag(id,etag)){
			
			return new ResponseEntity<>(HttpStatus.NOT_MODIFIED);
			
		} if(etag == null){
			Map<String,String> queryMap = redisConnection.getJedis().hgetAll(getParent(id)+"ETAG");

			

			for(Object v : queryMap.keySet()){

			if(v.equals("new") || v.equals("used")){

			etag = queryMap.get(v);

			}

			}
		} if(etag != null && !isUsedEtag(id,etag)){
			
			deleteEtag(id);
			redisConnection.getJedis().hset(getParent(id)+"ETAG", "used", etag);
			
		}
		
		
		
		
		
		
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
		hm = (HashMap) new JSONParser().parse(result.toString());
		
		if(result != null){
			JSONObject json = (JSONObject) new JSONParser().parse(result);
			if(!json.get("_type").equals(uriType)){
				result = null;
			}
		}
		
		HashMap reconstructedObject = reconstructHashMap(hm);
		
		JSONObject json = new JSONObject();
		json.putAll(reconstructedObject);
		
		MultiValueMap<String, String> params = new LinkedMultiValueMap<String, String>();

	    params.set("Etag", etag);
		
		return new ResponseEntity<Object>(result == null ? null : json,result == null ? null : params,result == null ? HttpStatus.FOUND :HttpStatus.NOT_FOUND);		
	}
	
	@RequestMapping(value="/{uriType}/{id}", method = RequestMethod.PATCH, consumes="application/json")
	public ResponseEntity<Object> patch(@RequestHeader HttpHeaders headers,@RequestBody String entity, @PathVariable String uriType,
			@PathVariable String id) throws ParseException, UnsupportedEncodingException, NoSuchAlgorithmException{
		
		if(!isAuthorized(headers)) return new ResponseEntity<Object>(HttpStatus.UNAUTHORIZED);
		
		String result = getJSONObject(id).toJSONString();
				//redisConnection.getJedis().hget(id, uriType);
		
		if(result == null) return new ResponseEntity<Object>(null, HttpStatus.NOT_FOUND);
		
		HashMap changes = (HashMap) new JSONParser().parse(entity);
		
		HashMap original = (HashMap) new JSONParser().parse(result);
		
		//Note : We will only change the String Values in fields not the JSON.
		//We will deal with fields present in original map
		
		System.out.println("Parent is :" + getParent(id));
		
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
		
		redisConnection.getJedis().hset(id,uriType + "," +getParent(id),json.toString());
		
		deleteEtag(getParent(id));
		
		
		return new ResponseEntity<Object>(json, generateNewEtag(json),HttpStatus.CREATED);
	}
	
	@RequestMapping(value="/{uriType}", method = RequestMethod.PUT, consumes="application/json")
	public ResponseEntity<Object> update(@RequestHeader HttpHeaders headers,@RequestBody String entity) throws ParseException, UnsupportedEncodingException, NoSuchAlgorithmException{
		
		if(!isAuthorized(headers)) return new ResponseEntity<Object>(HttpStatus.UNAUTHORIZED);
		
		JSONObject jsonObject = new JSONObject();
		JSONParser jsonParser = new JSONParser();
		jsonObject = (JSONObject) jsonParser.parse(entity);
		String key = (String) jsonObject.get("_id");
		String result = getJSONObject(key).toJSONString();
				//redisConnection.getJedis().get(key);
		if(result == null){
			return new ResponseEntity<Object>(key,HttpStatus.NOT_FOUND);
		}else{
			HashMap current = (HashMap) new JSONParser().parse(result);
			HashMap update = (HashMap) jsonObject;
			
			HashMap reconstructedObject = reconstructHashMap(current);
						
			HashMap result1 = updated(reconstructedObject,update);
			if(result1 != null){
				JSONObject newObject = new JSONObject();
				newObject.putAll(result1);
				//redisConnection.getJedis().set((String) newObject.get("_id"), newObject.toString());
				
				
				HashMap hm1 = makeGrandHashMap(result1,key);
				
				JSONObject jsonZ = new JSONObject();
				jsonZ.putAll(hm1);
				
				String key1 = jsonZ.get("_id").toString();
				
				//redisConnection.getJedis().set(key, jsonZ.toString());
				
				redisConnection.getJedis().hset(key1,jsonZ.get("_type").toString()+","+key1 ,jsonZ.toString());
				
				deleteEtag(getParent(key1));
				
				
				
				return new ResponseEntity<Object>(newObject,generateNewEtag(jsonZ),HttpStatus.ACCEPTED);
			}else{
				return new ResponseEntity<Object>(key,HttpStatus.NOT_FOUND);
			}
		}
	}
	
	public HashMap updated(HashMap hash1, HashMap hash2){
		for(Object o : hash1.keySet()){
			if(!hash1.get(o).equals(hash2.get(o))){
				hash1.put(o, hash2.get(o));
			}
		}
		return hash1;
		
	}

	
	@RequestMapping(value = "/{uriType}/{id}", method = RequestMethod.DELETE)
	public ResponseEntity<Void> delete(@RequestHeader HttpHeaders headers, @PathVariable String uriType,
			@PathVariable String id) throws ParseException{
		
		if(!isAuthorized(headers)) return new ResponseEntity<Void>(HttpStatus.UNAUTHORIZED);
		
		String result = getJSONObject(id).toJSONString();
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
	
	
	
	public void cascadeDelete(HashMap hm) throws ParseException {
		
	
		for(Object o : hm.keySet()){
			
			//do nothing studd
			
			if(o.toString().equals("_type")){
				
				System.out.println("We do nothing for type");

			} else 
				if(o.toString().equals("_id") ){
			
					redisConnection().getJedis().del(hm.get(o).toString());
					
					System.out.println("Deleting : " + hm.get(o).toString());
			
			} else {
				
				Object value = hm.get(o);
				
				if(value instanceof JSONArray){
					
					JSONArray jsonArray = (JSONArray) hm.get(o);
					//iterate over & call the method again
					
					for(int i = 0; i < jsonArray.size(); i++){
						String key = jsonArray.get(i).toString();
						
						if(redisConnection.getJedis().type(key.toString()).toString().equals("hash")){
							System.out.println("Its a hash get the document and pass it further please");
							
							
							JSONObject obj = getJSONObject(key);
							
							HashMap hm1  = (HashMap) obj;
							reconstructHashMap(hm1);
							
							
						}
						

					}
					
					
				} else if(value instanceof String){
					
					//retriving Map
										
					if(redisConnection.getJedis().type(value.toString()).toString().equals("hash")){
						
						JSONObject job = getJSONObject(value.toString());
						
						HashMap hm2  = (HashMap) job;
						
						System.out.println("Deleting : " + o.toString());
						
						reconstructHashMap(hm2);
						
						
					}
					
				}
				
			}
			
		}
	}
	
	


}
