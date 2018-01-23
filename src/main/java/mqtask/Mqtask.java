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
	    System.out.println(" [x] Task queue opened");
  }

  public void closeTaskQueue() throws IOException, TimeoutException {
	    channel.close();
	    connection.close();
	    System.out.println(" [x] Task queue closed");
  }

  public void storeMessage(String topic, String message) throws IOException {

	  if (message.length() < 1 || topic.length() < 1)
		  System.out.println(" [x] Empty message or topic");
	  else {
		  String jsonmsg = "{ \"topic\": \""+topic+"\",\"payload\": {"+message+"}}";
		  channel.basicPublish( "", TASK_QUEUE_NAME,
				  MessageProperties.PERSISTENT_TEXT_PLAIN,
				  jsonmsg.getBytes());
		  System.out.println(" [x] Sent '" + jsonmsg + "'");
	  }
	}
}
