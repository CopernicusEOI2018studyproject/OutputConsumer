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
    	LocalDateTime todayAt6 = LocalDateTime.now().withDayOfMonth(12).withHour(17).withMinute(0).withSecond(0).withNano(0);
    	
    	OutputConsumer consumerOutput = new OutputConsumer("OutputEvents","80.158.16.60:9092",todayAt6,"E:/testEnvironment/output9.json",false);
    	consumerOutput.runConsumer();
    }
}
