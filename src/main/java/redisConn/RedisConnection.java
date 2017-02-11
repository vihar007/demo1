package redisConn;

import redis.clients.jedis.Jedis;

public class RedisConnection {
	
	private Jedis jedis;
	
	public RedisConnection(){
		this.jedis = new Jedis("localhost");
		System.out.println("Ping " + jedis.ping().toString());
		this.jedis.flushDB();
	}
	
	public Jedis getJedis(){
		return this.jedis;
	}
	
       

}

