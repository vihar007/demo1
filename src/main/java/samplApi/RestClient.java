package samplApi;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

public class RestClient {

	private String CM = "http://192.168.2.58:8181/cm/";
	private org.springframework.web.client.RestTemplate rest;
	private HttpHeaders headers;
	private HttpStatus status;

	public RestClient() {
		this.rest = new RestTemplate();
		this.headers = new HttpHeaders();
		headers.add("Content-Type", "application/json");
		headers.add("Authorization",
				"Bearer ew0KICAgInVzZXJuYW1lIiA6ICJhc21pdGhAaGlnaHJvYWRzLmNvbSIsDQogICAib3JnYW5pemF0aW9uIiA6ICJoaWdocm9hZHMuY29tIg0KfQ==");
	}

	public String get(String uri) {
		HttpEntity<String> requestEntity = new HttpEntity<String>("", headers);
		ResponseEntity<String> responseEntity = rest.exchange(uri, HttpMethod.GET, requestEntity, String.class);
		this.setStatus(responseEntity.getStatusCode());
		return responseEntity.getBody();
	}

	public String post(String uri, String json) {
		HttpEntity<String> requestEntity = new HttpEntity<String>(json, headers);
		ResponseEntity<String> responseEntity = rest.exchange(uri, HttpMethod.POST, requestEntity, String.class);
		this.setStatus(responseEntity.getStatusCode());
		return responseEntity.getBody();
	}

	public String postTemplate(String uri, String plan) {
		// RestTemplate restTemplate = new RestTemplate();
		HttpEntity request = new HttpEntity(plan, headers);
		String Id = rest.postForObject(CM + uri, request, String.class);
		return Id;
	}

	public ResponseEntity<String> put(String uri, String json, String id) {
		Map<String, String> param = new HashMap<String, String>();
		param.put("id", id);
		HttpEntity<String> requestEntity = new HttpEntity<String>(json, headers);

		ResponseEntity<String> responseEntity = rest.exchange(uri, HttpMethod.PUT, requestEntity, String.class, param);
		this.setStatus(responseEntity.getStatusCode());
		return responseEntity;
	}

	public HttpStatus getStatus() {
		return status;
	}

	public void setStatus(HttpStatus status) {
		this.status = status;
	}

	public String storeInElasticSearch(String planId, String dequeuedPlan) throws IOException {
			System.out.println("You have reached");
		Index index = new Index.Builder(dequeuedPlan).index("plan_ind").type("s_p").id(planId).build();
		
		getClient().execute(index);
		System.out.println("Its done!!!!");
		return "";
	
	}
	
//	public void deleteIndex(String id) throws IOException {
//		Delete index = new Delete.Builder(id).index("plan_ind").type("s_p").build();
//		Consumer_Rabbit.getClient().execute(index);
//	}


	public JestClient getClient() {
		// Construct a new Jest client according to configuration via factory
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200").readTimeout(20000).connTimeout(6000).multiThreaded(true).build());
		JestClient client = factory.getObject();
		return client;
	}

}
