package mqworker;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.sql.DriverManager;
//import java.sql.ResultSet;
import org.json.JSONObject;
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

    System.out.println(" [x] Worker queue opened");
    
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
	    System.out.println(" [x] Worker queue closed");
}

  private void doWork(String task) throws Exception {
    if (task.length() > 0) {
    	int id = 0, value = 0;
    	String topic = "", stuff = "";
    	JSONObject jsonObject = null, payload = null;

    	try {
    		jsonObject = new JSONObject(task);
    		topic = jsonObject.getString("topic");
    	    payload = jsonObject.getJSONObject("payload");
    	} finally {

    	if (topic == null || payload == null) {
    		throw new org.json.JSONException(" [!] Illegal topic or payload");
    	}

    	if (topic.split("/").length < 3) {
    		throw new org.json.JSONException(" [!] Topic missing field(s)");
    	}

    	System.out.println("topic: "+topic);
    	System.out.println("payload: "+payload);

    	id = Integer.parseInt(topic.split("/")[2]);
    	stuff = topic.split("/")[1];

    	Class.forName("org.postgresql.Driver");
    	String url = "jdbc:postgresql://localhost/iot_db?user=postgres";
        java.sql.Connection conn = DriverManager.getConnection(url);
    	java.sql.PreparedStatement pstmt = null;

    	if (stuff.equals("temperature")) {
    		value = payload.getInt("temp");
    		System.out.println("temp: "+payload.getInt("temp"));

        	// create table testschema.tempsensor (id integer, time timestamp NOT NULL, value integer);
        	String sqlInsert = "INSERT INTO testschema.tempsensor (id,time,value) VALUES (?,?,?)";
        	java.sql.Timestamp sqlTime = new java.sql.Timestamp(Calendar.getInstance(TimeZone.getDefault()).getTimeInMillis());
        	pstmt = conn.prepareStatement(sqlInsert);
        	pstmt.setInt(1, id);
        	pstmt.setTimestamp(2, sqlTime);
        	pstmt.setInt(3, value);
        	pstmt.executeUpdate();
    	}

    	if (pstmt != null) {
    		pstmt.close();
    	}
        conn.close();
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
    	}
    }
  }
}
