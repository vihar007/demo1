import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory

import redis.clients.jedis.Jedis;

@Configuration
public class RedisConfig {
	
	@Bean
	public JedisConnectionFactory{
		JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory();
		jedisConnectionFactory.setHostName("localhost");
		jedisConnectionFactory.setPort(6379);
		
		return jedisConnectionFactory;

	}

}
