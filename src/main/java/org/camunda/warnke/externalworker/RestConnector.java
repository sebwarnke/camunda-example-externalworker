package org.camunda.warnke.externalworker;

import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * This class provides static methods to send REST requests to Camunda BPM. The two requests provided are: external-task/fetchAndLock and external-task/.../complete.
 * @author Sebastian Warnke
 *
 */
public class RestConnector {

	public static final Logger log = LoggerFactory.getLogger(RestConnector.class);

	private static final String REST_URL = "http://localhost:8080/engine-rest";

	public static JSONArray fetchWork() {

		JSONArray result = new JSONArray();
		
		String body = createFetchAndLockJsonBody();
		
		try {
			HttpResponse<JsonNode> httpResponse = Unirest.post(REST_URL + "/external-task/fetchAndLock")
					.header("content-type", "application/json").body(body).asJson();

			result = httpResponse.getBody().getArray();

		} catch (UnirestException e) {
			log.error("Cannot handle request 'external-task/fetchAndLock'.", e);
		}
		
		return result;

	}

	private static String createFetchAndLockJsonBody() {
		
		String result = "";
		
		ObjectMapper mapper = new ObjectMapper();
		
		ObjectNode mainNode = mapper.createObjectNode();

		mainNode.put("workerId", "aWorkerId");
		mainNode.put("maxTasks", 2);
		mainNode.put("usePriority", false);

		ObjectNode topicNode = mapper.createObjectNode();
		topicNode.put("topicName", "sampletopic");
		topicNode.put("lockDuration", 10000);

		ArrayNode topicArrayNode = mainNode.withArray("topics");
		topicArrayNode.add(topicNode);
		
		try {
			result = mapper.writeValueAsString(mainNode);
		} catch (JsonProcessingException e) {
			log.error("Cannot serialize fetchAndLock body", e);
		}
		
		return result;
	}

	public static void complete(String taskId) {

		ObjectMapper mapper = new ObjectMapper();
		ObjectNode mainNode = mapper.createObjectNode();
		mainNode.put("workerId", "aWorkerId");

		HttpResponse<JsonNode> httpResponse;
		try {
			httpResponse = Unirest.post(REST_URL + "/external-task/" + taskId + "/complete")
					.header("content-type", "application/json").body(mapper.writeValueAsString(mainNode)).asJson();

			int status = httpResponse.getStatus();
			
			if (status != 204) {
				log.error("Error completing task. Status: {}, {}", status, httpResponse.getStatusText());
			}
			
		} catch (JsonProcessingException e) {
			log.error("Cannot serialize 'complete' body", e);
		} catch (UnirestException e) {
			log.error("Cannot handle request 'external-task/complete'.", e);
		}

	}

}
