package samplApi;

import java.util.HashMap;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
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
	public ResponseEntity<Object> create(@RequestHeader HttpHeaders headers, @RequestBody String entity, @PathVariable String uriType) throws ParseException{
		
		JSONObject object = (JSONObject) new JSONParser().parse(entity);
		
		HashMap hm = new HashMap();
		hm = (HashMap) new JSONParser().parse(object.toString());
		
		for(Object o : hm.keySet()){
		System.out.println(o + "/"+hm.get(o));
		}
		
		if(hm.containsKey("$schema"))
			redisConnection.getJedis().set("schema^" + object.get("$schema").toString(), object.toString());
		
		if(hm.containsKey("_id"))
		redisConnection.getJedis().set(object.get("_id").toString(), object.toString());
			
		return new ResponseEntity<Object>(object, HttpStatus.CREATED);
	}
	
	@RequestMapping(value = "/{uriType}/{id}", method = RequestMethod.GET, produces = "application/json")
	public ResponseEntity<Object> get(@RequestHeader HttpHeaders headers, @PathVariable String uriType,
			@PathVariable String id) throws ParseException{
		String result = redisConnection.getJedis().get(id);
		HashMap hm = new HashMap();
		if(result != null)
		hm = (HashMap) new JSONParser().parse(result);
				
		return new ResponseEntity<Object>(result == null ? null : (JSONObject) new JSONParser().parse(result),result == null ? HttpStatus.FOUND :HttpStatus.NOT_FOUND);		
	}
	
	@RequestMapping(value = "/{uriType}/{id}", method = RequestMethod.DELETE, produces = "application/json")
	public ResponseEntity<Void> delete(@RequestHeader HttpHeaders headers, @PathVariable String uriType,
			@PathVariable String id) throws ParseException{
		
		
		String result = redisConnection.getJedis().get(id);
		if(result == null) 
		return new ResponseEntity<Void>(HttpStatus.NOT_FOUND);
		
		HashMap hm = new HashMap();
		hm = (HashMap) new JSONParser().parse(result);
		
		if(hm.containsKey("_id")) 
			redisConnection.getJedis().del(id);
		return new ResponseEntity<>(HttpStatus.OK);
	}

}
