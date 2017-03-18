package samplApi;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.embedded.EmbeddedServletContainerCustomizer;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SampleApiApp {
	
	@Bean
    public EmbeddedServletContainerCustomizer containerCustomizer() {
        return (container -> {
            container.setPort(9001);
        });
    }

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SpringApplication.run(SampleApiApp.class, args);
		
		
		//creating encryptor
		
		
		


	}

}
