package kafka.airportManager;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


public class AirportDepartureProducer {

	Properties props; // The producer is a Kafka client that publishes records to the Kafka cluster.

	KafkaProducer<String, String> producer;
	
//****************************************************************
// 1.KAFKA PRODUCER CONFIGURATION 
//****************************************************************
	AirportDepartureProducer() {
		
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


	void produceAndPrint(String topic,int zone,long []IDs, int batchSize,int start){

		Random rand = new Random();
		List<String> gatesList = Arrays.asList("A", "B", "C","D");
		List<String> failList = Arrays.asList("OK","REJ");
		String randomGate;
		String randomFail;
		
		for (int i = start; i <= batchSize; i++){
			// Fire-and-forget send(topic, key, value)
			// Send adds records to unsent records buffer and return
			
			// CHECK-IN --> STARTING POINT
			if (zone==0) {
				String msg = "{\"passenger_ID\":"+Long.toString(IDs[i])+"}";
			producer.send(new ProducerRecord<String, String>(topic,zone,"CheckIn",msg));
			}
			// SECURITY CHECK
			if (zone==1) {
				
			    randomGate = gatesList.get(rand.nextInt(gatesList.size()));
			    randomFail="OK";
			    if(i%10==0) 
			    {
			    	randomFail = failList.get(rand.nextInt(failList.size()));
			    }
				
				String msg="{\"passenger_ID\":"+Long.toString(IDs[i])+",\"gate\":\""+randomGate+"\",\"status\":\""+randomFail+"\"}"; 
			producer.send(new ProducerRecord<String, String>(topic,zone,"Security",msg));
			}
			// SHOPS
			if (zone==2) {
			String msg = "{\"passenger_ID\":"+Long.toString(IDs[i])+"}";
			producer.send(new ProducerRecord<String, String>(topic,zone,"Shops",msg));
			}
			
			// GATES --> LEAVING POINT
			if (zone==3) {
			
			randomGate = gatesList.get(rand.nextInt(gatesList.size()));
		    
			
			String msg="{\"passenger_ID\":"+Long.toString(IDs[i])+",\"gate\":\""+randomGate+"\"}";   
			producer.send(new ProducerRecord<String, String>(topic,zone,"GatesDep",msg));
			
			}
	}}

	void stop() {
		producer.close();
	}

	public static void main(String[] args) {
		
		int count=1000;
		double perc;
		int seed = 20;
		//Extremes of group of people
		int minBatch=10;
		int maxBatch=150;
		
		//Actual batch size 
		int batchSizeCheckIn=0;
		int batchSizeSecurity=0;
		int batchSizeShops=0;
		int batchSizeGates=0;
		//People that don't take the fly
		int batchRemainderGates=0;
		// Number of people of the actual 
		// batch size depending on the random percentage
		int effectiveSizeCheckIn;
		int effectiveSizeShops;
		int effectiveSizeGates;
		// Flag for check the initial condition
		boolean isFirstBatch=true;
		// Integer that keep track of the advancement
		// in the random sequence
		int sequenceTracker=0;
		int tmpTracker=0;
		
		Random intGenerator=new Random(seed);
		Random doubleGenerator=new Random(seed);
		long[] randomIDs = new long[10000];
		
		for(int i=0;i<randomIDs.length;i++) {
			
			randomIDs[i]=Integer.toUnsignedLong(intGenerator.nextInt());
		}
	
		AirportDepartureProducer myProducer = new AirportDepartureProducer();
		while(count>0) {
		if(isFirstBatch){
			
			perc=doubleGenerator.nextDouble();
			batchSizeCheckIn=intGenerator.nextInt((maxBatch-minBatch)+1)+minBatch;
			effectiveSizeCheckIn=(int) Math.floor(batchSizeCheckIn*perc);
			
			batchSizeSecurity=intGenerator.nextInt((maxBatch-minBatch)+1)+minBatch;
			
			perc=doubleGenerator.nextDouble();
			batchSizeShops= intGenerator.nextInt((maxBatch-minBatch)+1)+minBatch;
			effectiveSizeShops=(int) Math.floor(batchSizeShops*perc);
			
			perc=doubleGenerator.nextDouble();
			batchSizeGates= intGenerator.nextInt((maxBatch-minBatch)+1)+minBatch;
			effectiveSizeGates=(int) Math.floor(batchSizeGates*perc);
			batchRemainderGates=0;
			
			sequenceTracker=batchSizeCheckIn+batchSizeSecurity+batchSizeShops+batchSizeGates;
			isFirstBatch=false;
		
		}
		else{
			batchSizeGates= batchSizeShops;
			batchSizeShops= batchSizeSecurity;
			batchSizeSecurity=batchSizeCheckIn;
			
			perc=doubleGenerator.nextDouble();
			effectiveSizeShops=(int) Math.floor(batchSizeShops*perc);
			perc=doubleGenerator.nextDouble();
			effectiveSizeGates=(int) Math.floor(batchSizeGates*perc);
			
			batchRemainderGates=batchSizeGates-effectiveSizeGates;
			
			perc=doubleGenerator.nextDouble();
			batchSizeCheckIn=intGenerator.nextInt((maxBatch-minBatch)+1)+minBatch;
			effectiveSizeCheckIn=(int) Math.floor(batchSizeCheckIn*perc);
			
			sequenceTracker+=batchSizeCheckIn;
			if(sequenceTracker>10000) {
				seed+=1;
				intGenerator=new Random(seed);
				for(int i=0;i<randomIDs.length;i++) {
					
					randomIDs[i]=Integer.toUnsignedLong(intGenerator.nextInt());
				}
				sequenceTracker=batchSizeCheckIn+batchSizeSecurity+batchSizeShops+batchSizeGates;
			}
		}
		
		tmpTracker=sequenceTracker;
		
		myProducer.produceAndPrint("AirportDep",0,randomIDs,tmpTracker,tmpTracker-effectiveSizeCheckIn);
		
		tmpTracker-=batchSizeCheckIn;
		try {
		    Thread.sleep(5000);
		    } catch (InterruptedException e) {
					continue;
				}
		myProducer.produceAndPrint("AirportDep",1,randomIDs,tmpTracker,tmpTracker-batchSizeSecurity);
		tmpTracker-=batchSizeSecurity;
		try {
		    Thread.sleep(5000);
		    } catch (InterruptedException e) {
					continue;
				}
		myProducer.produceAndPrint("AirportDep",2,randomIDs,tmpTracker,tmpTracker-effectiveSizeShops);
		tmpTracker-=batchSizeShops;
		try {
		    Thread.sleep(5000);
		    } catch (InterruptedException e) {
					continue;
				}
		myProducer.produceAndPrint("AirportDep",3,randomIDs,tmpTracker,tmpTracker-effectiveSizeGates-batchRemainderGates);
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
