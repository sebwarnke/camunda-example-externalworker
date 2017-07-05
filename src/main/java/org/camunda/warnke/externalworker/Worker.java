package org.camunda.warnke.externalworker;

import static org.camunda.spin.Spin.JSON;

import org.camunda.spin.json.SpinJsonNode;

/**
 * The worker processes one job that was passed to him as JSON string.
 * @author Sebastian Warnke
 *
 */
public class Worker implements Runnable {
	
//	private final static Logger log = LoggerFactory.getLogger(Worker.class);
	
	private String payload = "";
	
	private String processDefinitionKey = "";
	private String processInstanceId = "";
	private String taskId = "";

	public Worker(String payload) {
		this.payload = payload;
		
		SpinJsonNode jsonPayload = JSON(this.payload);
		SpinJsonNode processDefinitionKeyProperty = jsonPayload.prop("processDefinitionKey");
		SpinJsonNode processInstanceIdProperty = jsonPayload.prop("processInstanceId");
		SpinJsonNode taskIdProperty = jsonPayload.prop("id");
		
		this.processDefinitionKey = processDefinitionKeyProperty.stringValue();
		this.processInstanceId = processInstanceIdProperty.stringValue();
		this.taskId = taskIdProperty.stringValue();
	}
	
	public void work() {
		
		System.out.println("Process Definition Key: " + processDefinitionKey);
		System.out.println("Process Instance ID: " + processInstanceId);
		System.out.println("Task ID: " + taskId);
		
		RestConnector.complete(taskId);
	}

	public void run() {
		work();
	}
}
