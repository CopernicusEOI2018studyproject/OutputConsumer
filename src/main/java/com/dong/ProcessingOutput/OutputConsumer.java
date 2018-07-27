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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.notgroupb.formats.OutputDataPoint;
import org.notgroupb.formats.deserialize.OutputDataPointDeserializer;

public class OutputConsumer {
	private  static String TOPIC = ""; // topic which is subscribed
    private  static String BOOTSTRAP_SERVERS = "";
    private static List<ConsumerRecord> outputRecord = new ArrayList<ConsumerRecord>();
    private static List<OutputDataPoint> output8RecordNum = new ArrayList<OutputDataPoint>(); //store the numerator
    private static List<OutputDataPoint> output8RecordDe = new ArrayList<OutputDataPoint>(); //store the denominator
    private static List<OutputDataPoint> outputCurrentRecord = new ArrayList<OutputDataPoint>(); //store current score 
    private static LocalDateTime  targetTime;
    private static Boolean isContain = false;
    private static String outputPath;
   
	public OutputConsumer(String subscribeTopic, String ipPort, LocalDateTime targettime, String outputpath)
    {
    	TOPIC = subscribeTopic;
    	BOOTSTRAP_SERVERS = ipPort;
    	targetTime = targettime;
    	outputPath = outputpath +targettime.toLocalDate().toString() +"T" +targettime.getHour()+".json";
    }
    public Consumer<String, OutputDataPoint> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"KafkaExampleConsumer" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                OutputDataPointDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Create the consumer using props.
        final Consumer<String, OutputDataPoint> consumer = new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }
    public void runConsumer() throws InterruptedException {
        final Consumer<String, OutputDataPoint> consumer = createConsumer();
        final int giveUp = 500;   
        int noRecordsCount = 0;
        
        while (true) {
        	// find out what happened in this fucntion  consumer.poll
             ConsumerRecords<String, OutputDataPoint> consumerRecords =
                    consumer.poll(100); //in milliseconds and 2mins it should be 120000 ,If no records are available after the time period specified, the poll method returns an empty ConsumerRecords.
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                System.out.println("no records for " + noRecordsCount +"times");
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            System.out.println("how many records? " + consumerRecords.count() +"times");
            
            consumerRecords.forEach(record -> {
            	System.out.println("what is  record? " + record.value().toString());
            	//for each record what's the processing?  first one find out the biggest one. 
            	//step one: filter data inside this time span 
            	//when the new records get inside, compare to the name of list, and the value, and replace if the new value is larger than old one.
            	//when it comes to hour/ report  the list and clean the list.
            	double offset =  Duration.between(targetTime, (Temporal) record.value().getRecordTime()).getSeconds();
///
            	// code for next one hour
            	if (offset <= 3600 && offset > 0 )
            	{
            		isContain = false;
            		for(int p=0;p<outputRecord.size();p++)
            		{
            			ConsumerRecord element = outputRecord.get(p);
            			if(element.key().toString().equals(record.key().toString()))
            			{
            				isContain = true;
            				if(((OutputDataPoint)element.value()).getScore() < record.value().getScore())
            				{
            					outputRecord.remove(element);
            					outputRecord.add(record);
            					break;
            				}
            			}
            		}
            		if (isContain == false)
    				{
    					outputRecord.add(record);
    				}
            	}
///////////////////////////////////////////////////////////////////////////////////////////////
            	// the code for last 8 hours
            	
                	if(offset >= -28800 && offset < 0)
                	{
                		isContain = false;
                		// 
                		for(int p =0; p<output8RecordNum.size(); p++)
                		{
                			if(output8RecordNum.get(p).getName().toLowerCase().toString().equals( record.key().toLowerCase().toString()))
                			{
                				isContain = true;
                				//IDW algorithm
                				double numerator = output8RecordNum.get(p).getScore() + (record.value().getScore()*10000)/(offset*offset);
                				output8RecordNum.get(p).setScore(numerator);

                				double denominator = output8RecordDe.get(p).getScore() + 10000/(offset*offset);
                				output8RecordDe.get(p).setScore(denominator);
                				break;
                			}
                		}
                		if (isContain == false)
        				{
                			double numerator = (record.value().getScore()*10000)/(offset*offset);
                			double denominator = 10000/(offset*offset);
                			OutputDataPoint tempDataPoint = new OutputDataPoint();
                			tempDataPoint.setName(record.value().getName());
                			tempDataPoint.setRecordTime(record.value().getRecordTime());
                			tempDataPoint.setLat(record.value().getLat());
                			tempDataPoint.setLon(record.value().getLon());
                			tempDataPoint.setScore(numerator);
                			output8RecordNum.add(tempDataPoint);

                			OutputDataPoint tempDataPoint2 = new OutputDataPoint();
                			tempDataPoint2.setName(record.value().getName());
                			tempDataPoint2.setRecordTime(record.value().getRecordTime());
                			tempDataPoint2.setLat(record.value().getLat());
                			tempDataPoint2.setLon(record.value().getLon());
                			tempDataPoint2.setScore(denominator);
                			output8RecordDe.add(tempDataPoint2);
        				}
                		
//                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
//                            record.key(), record.value(),
//                            record.partition(), record.offset());
            	}
            	

   ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            });
         // if the time is hour output the outputRecord// write in json file.
        	LocalDateTime currentTime = LocalDateTime.now();
        	if (currentTime.getMinute()>0)//57 at what time output a map
        	{
        		//for 8 hours output this list and clean it  
        		for(int p =0; p<output8RecordNum.size(); p++)
        		{
        			double temp = output8RecordNum.get(p).getScore() / output8RecordDe.get(p).getScore();
        			OutputDataPoint tempDataPoint = new OutputDataPoint();
        			tempDataPoint = (OutputDataPoint) output8RecordNum.get(p);
        			tempDataPoint.setScore(temp);
        			outputCurrentRecord.add(tempDataPoint);
        		}
        		//for last one hour 
        		FileWriter writer = new FileWriter();
        		writer.write(outputRecord,outputCurrentRecord,outputPath);
        		outputCurrentRecord.clear();
        	}
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

}
