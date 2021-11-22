package kafka.airportManager;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


public class ParkingLotsProducer {

	Properties props; // The producer is a Kafka client that publishes records to the Kafka cluster.

	KafkaProducer<String, String> producer;
	
//****************************************************************
// 1.KAFKA PRODUCER CONFIGURATION 
//****************************************************************
	ParkingLotsProducer() {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put(ProducerConfig.ACKS_CONFIG,"all");
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


	void produceAndPrint(String topic,long []IDs, int batchSize,int start){

		Random rand = new Random();
		
		List<String> statusList = Arrays.asList("ENTER","EXIT");
		
		int partition=0;
		
		for (int i = start; i <= batchSize; i++){
			// Fire-and-forget send(topic, key, value)
			// Send adds records to unsent records buffer and return
			String status=statusList.get(rand.nextInt(statusList.size()));
			if(status=="ENTER") {partition=0;}			
			if(status=="EXIT") {partition=1;}
			String msg="{\"ticket_ID\":"+Long.toString(IDs[i])+",\"status\":\""+status+"\"}";
			producer.send(new ProducerRecord<String, String>(topic,partition,"ParkingLots",msg));
			
	}}

	void stop() {
		producer.close();
	}

	public static void main(String[] args) {
		
		int count=1000;
		int seed = 27;
		//Extremes of group of people
		int minBatch=10;
		int maxBatch=50;
		
		//Actual batch size 
		int batchSize=0;
		
		// Integer that keep track of the advancement
		// in the random sequence
		int sequenceTracker=0;
		
		Random intGenerator=new Random(seed);
		long[] randomIDs = new long[10000];
		
		for(int i=0;i<randomIDs.length;i++) {
			
			randomIDs[i]=Integer.toUnsignedLong(intGenerator.nextInt());
		}
	
		ParkingLotsProducer myProducer = new ParkingLotsProducer();
		while(count>0) {


		batchSize=intGenerator.nextInt((maxBatch-minBatch)+1)+minBatch;
		sequenceTracker+=batchSize;
		if(sequenceTracker>10000) {
				seed+=1;
				intGenerator=new Random(seed);
				for(int i=0;i<randomIDs.length;i++) {
					
					randomIDs[i]=Integer.toUnsignedLong(intGenerator.nextInt());
				}
				sequenceTracker=batchSize;
			}
			
			
		myProducer.produceAndPrint("ParkingLots",randomIDs,sequenceTracker,sequenceTracker-batchSize);
		

		try {
		    Thread.sleep(10000);
		    } catch (InterruptedException e) {
					continue;
				}
		
		
		count-=1;
		
	}
		myProducer.stop();

	}	
		
}	
	
	
	
