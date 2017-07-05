package org.camunda.warnke.externalworker;

import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ExternalWorkerApplication periodically looks for external tasks provided by Camunda BPM. Open jobs are fetched and passed
 * to worker threads one by one.
 * @author Sebastian Warnke
 *
 */
public class ExternalWorkerApplication {

	private static final Logger log = LoggerFactory.getLogger(ExternalWorkerApplication.class);
	
	public static void main(String[] args) throws InterruptedException {
		
		JSONArray foundwork = new JSONArray();
		
		do {
			Thread.sleep(5000);
			
			log.info("Looking for work.");
			foundwork = RestConnector.fetchWork();
			log.info("Fetched {} jobs.", foundwork.length());
			
			for (int i = 0; i < foundwork.length(); i++) {
				log.info("Starting worker thread.");
				new Thread(new Worker(foundwork.getJSONObject(i).toString())).run();
			}
			
		} while (true);
		
	}

}
