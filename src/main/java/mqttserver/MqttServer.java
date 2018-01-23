package mqttserver;

import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import mqtask.Mqtask;
import mqworker.Mqworker;

public class MqttServer implements MqttCallback {

	private MqttAsyncClient myClient;
	private MqttConnectOptions connOpt;
	private static Mqtask msgTask;
	private static Mqworker msgWorker;

	private static final String BROKER_URL = "tcp://localhost:1883";
	private static final String M2MIO_CLIENTID = "1234";
	private static final String M2MIO_DOMAIN = "odroid";	// mandatory
	private static final String M2MIO_STUFF = "#";			// mandatory
	private static final String M2MIO_THING = "";			// optional
	private static final String M2MIO_USERNAME = "";		// optional
	private static final String M2MIO_PASSWORD_MD5 = "";	// optional

	// the following two flags control whether this example is a publisher, a subscriber or both
	private static final Boolean subscriber = true;
	private static final Boolean publisher = true;

	
	public void connectComplete(boolean reconnect, String serverURI) {
		System.out.println("Connection completed to "+serverURI+", isReconnection: "+reconnect);
	}

	/**
	 * 
	 * connectionLost
	 * This callback is invoked upon losing the MQTT connection.
	 * 
	 */
	public void connectionLost(Throwable t) {
		System.out.println("Connection lost!");
//		startServer();
	}

	/**
	 * 
	 * deliveryComplete
	 * This callback is invoked when a message published by this client
	 * is successfully received by the broker.
	 * 
	 */
	public void deliveryComplete(IMqttDeliveryToken token) {
		try {
			System.out.println("Completed delivery token: "+token);
			System.out.println("Pub complete" + new String(token.getMessage().getPayload()));
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * messageArrived
	 * This callback is invoked when a message is received on a subscribed topic.
	 * 
	 */
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		System.out.println("-------------------------------------------------");
		System.out.println("| Topic: " + topic);
		System.out.println("| Message: " + new String(message.getPayload()));
		System.out.println("-------------------------------------------------");
		msgTask.storeMessage(topic, new String(message.getPayload()));
	}
//    smc.publishMessage("Bye, bye");
	/**
	 * 
	 * MAIN
	 * @throws Throwable 
	 * 
	 */
	public static void main(String[] args) throws Exception {
		MqttServer smc = new MqttServer();
		msgTask = new Mqtask();
		msgWorker = new Mqworker();
		Scanner scan = new Scanner(System.in);
		String inputString = "";

		try {
			msgWorker.startQueueWorker();
			msgTask.openTaskQueue();
		} catch (TimeoutException e) {
			e.printStackTrace();
		} finally {
			smc.startServer();
			smc.subscribeTopic();
		
		    while (!"Bye".equals(inputString)) {
		    	System.out.println("Bye exists");
		    	try {
		    	inputString = scan.nextLine();
		    	} catch (Exception e) {
		    		// ignore
		    	}
		    }

		    scan.close();
		    smc.unsubscribeTopic();
			smc.stopServer();

			try {
				msgTask.closeTaskQueue();
				msgWorker.closeQueueWorker();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 
	 * startServer
	 * The main functionality of this simple example.
	 * Create a MQTT client, connect to broker, pub/sub, disconnect.
	 * 
	 */
	public void startServer( ) {
		// setup MQTT Client
		String clientID = M2MIO_CLIENTID;
		connOpt = new MqttConnectOptions();		
		connOpt.setCleanSession(true);
		connOpt.setKeepAliveInterval(30);
		if (!"".equals(M2MIO_USERNAME)) {
			connOpt.setUserName(M2MIO_USERNAME);
		}
		if (!"".equals(M2MIO_PASSWORD_MD5)) {
			connOpt.setPassword(M2MIO_PASSWORD_MD5.toCharArray());
		}
		
		// Connect to Broker
		try {
			myClient = new MqttAsyncClient(BROKER_URL, clientID);
			myClient.setCallback(this);
			myClient.connect(connOpt).waitForCompletion();
		} catch (MqttException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		System.out.println("Connected to " + BROKER_URL);
	}

	public void stopServer() {
		// disconnect
		try {
			myClient.disconnect().waitForCompletion();
			myClient.close();
		} catch (MqttException e) {
			e.printStackTrace();
		}
		
		System.out.println("Disconnected from " + BROKER_URL);
	}

	public void subscribeTopic() {
		// setup topic
		// topics on m2m.io are in the form <domain>/<stuff>/<thing>
		String myTopic = M2MIO_DOMAIN + "/" + M2MIO_STUFF;
		if (!"".equals(M2MIO_THING)) {
			myTopic = myTopic + "/" + M2MIO_THING;
		}

		// subscribe to topic if subscriber
		if (subscriber) {
			try {
				int subQoS = 0;
				myClient.subscribe(myTopic, subQoS).waitForCompletion();
			} catch (MqttException e) {
				e.printStackTrace();
			}
			System.out.println("subscribeTopic: "+myTopic);
		}
	}

	public void unsubscribeTopic() {
		// setup topic
		// topics on m2m.io are in the form <domain>/<stuff>/<thing>
		String myTopic = M2MIO_DOMAIN + "/" + M2MIO_STUFF;
		if (!"".equals(M2MIO_THING)) {
			myTopic = myTopic + "/" + M2MIO_THING;
		}

		// subscribe to topic if subscriber
		if (subscriber) {
			try {
				myClient.unsubscribe(myTopic).waitForCompletion();
			} catch (MqttException e) {
				e.printStackTrace();
			}
			System.out.println("unsubscribeTopic: "+myTopic);
		}
	}

	public void publishMessage(String aPubMsg) {
		// publish messages if publisher
		String myTopic = M2MIO_DOMAIN + "/" + M2MIO_STUFF;
		if (!"".equals(M2MIO_THING)) {
			myTopic = myTopic + "/" + M2MIO_THING;
		}

		if (publisher) {
		   		String pubMsg = "{\""+aPubMsg+"\"}";
		   		int pubQoS = 0;
				MqttMessage message = new MqttMessage(pubMsg.getBytes());
		    	message.setQos(pubQoS);
		    	message.setRetained(false);

		    	// Publish the message
		    	System.out.println("Publishing to topic \"" + myTopic + "\" qos " + pubQoS);
		    	IMqttDeliveryToken token = null;
		    	try {
		    		// publish message to broker
					token = myClient.publish(myTopic, message);
					System.out.println("Pending delivery token: "+token);
			    	// Wait until the message has been delivered to the broker
					token.waitForCompletion();
					Thread.sleep(100);
				} catch (Exception e) {
					e.printStackTrace();
				}	
		}
	}
}
