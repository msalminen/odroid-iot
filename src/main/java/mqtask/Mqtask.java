package mqtask;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

public class Mqtask {

  private static final String TASK_QUEUE_NAME = "iot_queue";
  private ConnectionFactory factory;
  private Connection connection;
  private Channel channel;

  public void openTaskQueue() throws IOException, TimeoutException {
	    factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    connection = factory.newConnection();
	    channel = connection.createChannel();
	    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
  }

  public void closeTaskQueue() throws IOException, TimeoutException {
	    channel.close();
	    connection.close();	  
  }

  public void storeMessage(String topic, String message) throws IOException {

	  if (message.length() < 1 || topic.length() < 1)
		  System.out.println(" [x] Empty message or topic");
	  else {

		  channel.basicPublish( "", TASK_QUEUE_NAME,
				  MessageProperties.PERSISTENT_TEXT_PLAIN,
				  message.getBytes());
		  System.out.println(" [x] Sent '" + message + "'");
	  }
	}
}
