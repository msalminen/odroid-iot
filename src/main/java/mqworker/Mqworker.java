package mqworker;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.sql.DriverManager;
//import java.sql.ResultSet;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.Calendar;
import java.util.TimeZone;

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
    	JSONParser jsonParser = new JSONParser();
    	JSONObject jsonObject = (JSONObject) jsonParser.parse(task);
    	String topic = (String)jsonObject.get("topic");
    	JSONObject payload = (JSONObject)jsonObject.get("payload");

    	System.out.println("topic: "+topic);
    	System.out.println("temp: "+payload.get("temp"));

    	// if(topic)
    		// do something
    	// if(payload)
    		// do something
    	
    	Class.forName("org.postgresql.Driver");
    	String url = "jdbc:postgresql://localhost/iot_db?user=postgres";
        java.sql.Connection conn = DriverManager.getConnection(url);
    	java.sql.PreparedStatement pstmt = null;

    	// create table testschema.tempsensor (id integer, time timestamp NOT NULL, value integer);
    	String sqlInsert = "INSERT INTO testschema.tempsensor (id,time,value) VALUES (?,?,?)";
    	java.sql.Timestamp sqlTime = new java.sql.Timestamp(Calendar.getInstance(TimeZone.getDefault()).getTimeInMillis());
    	pstmt = conn.prepareStatement(sqlInsert);
    	pstmt.setInt(1, 1);
    	pstmt.setTimestamp(2, sqlTime);
    	pstmt.setInt(3, 0);
    	pstmt.executeUpdate();
/*
    	java.sql.Statement stmt = conn.createStatement();
    	String sqlQuery = "SELECT id, time, value FROM testschema.tempsensor";
        ResultSet result = stmt.executeQuery(sqlQuery);

        while(result.next()){
           //Retrieve by column name
           int id  = result.getInt("id");
           java.sql.Timestamp timestamp = result.getTimestamp("time");
           int value = result.getInt("value");

           //Display values
           System.out.print("ID: " + id);
           System.out.print(", timestamp: " + timestamp);
           System.out.print(", value: " + value);
        }

        result.close();
        stmt.close();
 */
        pstmt.close();
        conn.close();
    }
  }
}
