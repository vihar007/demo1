package samplApi;

import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.simple.parser.JSONParser;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SampleController {
	
	@RequestMapping("/")
	public String sayhi(){
		JSONParser parser = new JSONParser();
		
		try{
			
			String json = new String(Files.readAllBytes(Paths.get("sample.txt")));
			
			String schema = new String(Files.readAllBytes(Paths.get("sampleSchema.txt"))); 	
			
			JSonValidation jSonValidation = new JSonValidation();
			
			return jSonValidation.validate(json, schema) == Boolean.TRUE ? "Valid" : "Invalid";
			
			
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "Error";
		
		
		
	}

}
