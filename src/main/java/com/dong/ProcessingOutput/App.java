package com.dong.ProcessingOutput;

import java.time.LocalDateTime;

/**
 * Hello world!
 *
 */
public class App 
{
	
    public static void main( String[] args ) throws Exception 
    {
    	//create 4 hour's historical map
    	LocalDateTime todayAt6 = LocalDateTime.now().withDayOfMonth(12).withHour(17).withMinute(0).withSecond(0).withNano(0);
    	OutputConsumer consumerOutput = new OutputConsumer("OutputEvents","80.158.16.60:9092",todayAt6,"E:/testEnvironment/",false);
    	consumerOutput.runConsumer();
 /*
    	//when current time == mins 55; create a current consumer
    	LocalDateTime todayNow = LocalDateTime.now().withMinute(0).withSecond(0).withNano(0);
    	// in the consumer,  when start compare to current time, if current time equal setting time, then flag = true, output the list "output8records"
    	OutputConsumer consumerNowOutput = new OutputConsumer("OutputEvents","80.158.16.60:9092",todayNow,"E:/testEnvironment/",true);
    	consumerNowOutput.runConsumer();
*/
    	
 
    	
    }
}
