package com.dong.ProcessingOutput;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.notgroupb.dataPreformatter.formats.OutputDataPoint;

public class OutputConsumer {
	private  static String TOPIC = "output-topic";
    private  static String BOOTSTRAP_SERVERS =
            "localhost:9092,localhost:9093,localhost:9094";
    private static List<ConsumerRecord> outputRecord = new ArrayList<ConsumerRecord>();
    private static LocalDateTime  targetTime;
    private static Boolean isContain = false;
    private static String outputPath;
    
    public void outputConsumer(String subscribeTopic, String ipPort, LocalDateTime targettime, String outputpath)
    {
    	TOPIC = subscribeTopic;
    	BOOTSTRAP_SERVERS = ipPort;
    	targetTime = targettime;
    	outputPath = outputpath;
    }
    public Consumer<String, OutputDataPoint> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // Create the consumer using props.
        final Consumer<String, OutputDataPoint> consumer = new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }
    public void runConsumer() throws InterruptedException {
        final Consumer<String, OutputDataPoint> consumer = createConsumer();
        final int giveUp = 100;   int noRecordsCount = 0;
        while (true) {
            final ConsumerRecords<String, OutputDataPoint> consumerRecords =
                    consumer.poll(300); //in seconds and 5mins,If no records are available after the time period specified, the poll method returns an empty ConsumerRecords.
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
            	//for each record what's the processing?  first one find out the biggest one. 
            	//step one: filter data inside this time span 
            	//when the new records get inside, compare to the name of list, and the value, and replace if the new value is larger than old one.
            	//when it comes to hour/ report  the list and clean the list.
            	double offset =  Duration.between(targetTime, (Temporal) record.value().getRecordTime()).getSeconds();
            	
            	if (offset < 3600 && offset > 0 )
            	{
            		isContain = false;
            		outputRecord.forEach( element ->{
            			if(element.key() == record.key())
            			{
            				isContain = true;
            				if(((OutputDataPoint)element.value()).getScore() > record.value().getScore())
            				{
            					outputRecord.remove(element);
            					outputRecord.add(record);
            				}
            				
            			}
            			
            		});
            		if (isContain == false)
    				{
    					outputRecord.add(record);
    				}
            	}
            	
            	// if the current time
//                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
//                        record.key(), record.value(),
//                        record.partition(), record.offset());
            });
         // if the time is hour output the outputRecord// write in json file.
        	LocalDateTime currentTime = LocalDateTime.now();
        	if (currentTime.getMinute()>55)
        	{
        		//output this list and clean it
        		FileWriter writer = new FileWriter();
        		writer.write(outputRecord, outputPath);
        	}
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

}
