package kafka.example1;
import java.util.Arrays;
import java.util.List;
/**
 * Simple producer 
 * @Author Isabel Muñoz
 */
import java.util.Properties;
import java.util.Random;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class SimpleProducer {

	Properties props; // The producer is a Kafka client that publishes records to the Kafka cluster.

	KafkaProducer<String, String> producer;
	
//****************************************************************
// 1.KAFKA PRODUCER CONFIGURATION 
//****************************************************************
	SimpleProducer() {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		// Serializer for conversion the key type to bytes
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		// Serializer for conversion the value type to bytes
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<String, String>(props);
	}
	
//****************************************************************
//2. TOPIC PRODUCTION
//****************************************************************	


	void produceAndPrint(String topic,int zone,int seed, int iteration,int start){
		Random generator = new Random(seed);
		if(zone==0 || zone ==2) {
			double perc = generator.nextDouble();
			iteration= (int) Math.floor(iteration*perc);
			
		}
		
		
		for (int i = start; i < iteration; i++){
			// Fire-and-forget send(topic, key, value)
			// Send adds records to unsent records buffer and return
			
			// CHECK-IN --> STARTING POINT
			if (zone==0) {
				String msg = "{\"passenger_ID\":"+Integer.toString(generator.nextInt())+",\"area\":0}";
			producer.send(new ProducerRecord<String, String>(topic,Integer
					.toString(generator.nextInt()),"VALUE CHECK-IN : "+msg));
			}
			// SECURITY CHECK
			if (zone==1) {
				List<String> givenList = Arrays.asList("A", "B", "C","D");
			    Random rand = new Random();
			    String randomGate = givenList.get(rand.nextInt(givenList.size()));
				
				String msg="{\"passenger_ID\":"+Integer.toString(generator.nextInt())+",\"area\":1,\"gate\":"+randomGate+"}"; 
			producer.send(new ProducerRecord<String, String>(topic,Integer
					.toString(generator.nextInt()),"VALUE SECURITY CHECK: "+msg));
			}
			// SHOPS
			if (zone==2) {
			String msg = "{\"passenger_ID\":"+Integer.toString(generator.nextInt())+",\"area\":2}";
			producer.send(new ProducerRecord<String, String>(topic,Integer
					.toString(generator.nextInt()),"VALUE SHOPS : "+msg));
			}
			
			// GATES --> LEAVING POINT
			if (zone==3) {
			List<String> givenList = Arrays.asList("A", "B", "C","D");
		    Random rand = new Random();
		    String randomGate = givenList.get(rand.nextInt(givenList.size()));
			
			String msg="{\"passenger_ID\":"+Integer.toString(generator.nextInt())+",\"area\":3,\"gate\":"+randomGate+"}";   
			producer.send(new ProducerRecord<String, String>(topic,Integer
					.toString(generator.nextInt()),"VALUE GATES: "+msg));
			
			}
	}}

	void stop() {
		producer.close();
	}

	public static void main(String[] args) {
		int count=10;
		//System.setProperty("kafka.logs.dir", "/home/isabel/kafka/logs");
		SimpleProducer myProducer = new SimpleProducer();
		while(count>0) {
		myProducer.produceAndPrint("TEST",0,20,50+50*count,0);
		
		try {
		    Thread.sleep(5000);
		    } catch (InterruptedException e) {
					continue;
				}
		myProducer.produceAndPrint("TEST2",1,20,50+50*(count+1),50+50*count);
		
		try {
		    Thread.sleep(5000);
		    } catch (InterruptedException e) {
					continue;
				}
		myProducer.produceAndPrint("TEST3",2,20,50+50*(count+2),50+50*count);
		
		try {
		    Thread.sleep(5000);
		    } catch (InterruptedException e) {
					continue;
				}
		myProducer.produceAndPrint("TEST4",3,20,50+50*(count+3),50+50*count);
		
		try {
		    Thread.sleep(5000);
		    } catch (InterruptedException e) {
					continue;
				}
		
		
		
		
		
		count-=1;
		}
		myProducer.stop();

	}
}
