package mqworker;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.sql.DriverManager;

public class Mqworker {

	private static final String TASK_QUEUE_NAME = "iot_queue";
	private com.rabbitmq.client.ConnectionFactory factory;
	private com.rabbitmq.client.Connection connection;
	private com.rabbitmq.client.Channel channel;

  public void startQueueWorker() throws Exception {
    factory = new ConnectionFactory();
    factory.setHost("localhost");
    connection = factory.newConnection();
    channel = connection.createChannel();

    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
    channel.basicQos(1);

    final com.rabbitmq.client.Consumer consumer = new com.rabbitmq.client.DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");

        System.out.println(" [x] Received '" + message + "'");
        try {
          doWork(message);
        } catch (Exception e) {
			e.printStackTrace();
		} finally {
          System.out.println(" [x] Done");
          channel.basicAck(envelope.getDeliveryTag(), false);
        }
      }
    };

    boolean autoAck = false;
    channel.basicConsume(TASK_QUEUE_NAME, autoAck, consumer);
  }

  public void closeQueueWorker() throws Exception {
	    channel.close();
	    connection.close();	  
}

  private void doWork(String task) throws Exception {
    if (task.length() > 0) {
    	String url = "jdbc:postgresql://localhost/iot_db?user=postgres";
    	java.sql.Connection conn = DriverManager.getConnection(url);
    	java.sql.Statement stmt = conn.createStatement();
    	//if ()
    	String sql = "INSERT INTO TEST (ID,NAME,AGE,ADDRESS,SALARY) "
           + "VALUES (1, 'Paul', 32, 'California', 20000.00 );";
        stmt.executeUpdate(sql);
    	// add_del_update_db_record("insert into DHT22_Temperature_Data (SensorID, Date_n_Time, Temperature) values (?,?,?)",[SensorID, Data_and_Time, Temperature])
    	
    }
  }
}
