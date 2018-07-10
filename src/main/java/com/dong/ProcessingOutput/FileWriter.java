package com.dong.ProcessingOutput;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FileWriter {
	//output the list
	public void fileWriter()
	{
		
	}
	public void write(List<ConsumerRecord> pointList, String path)
	{
		
		try {
			PrintWriter writer = new PrintWriter(path, "UTF-8");
			writer.println("[");
			pointList.forEach(record ->{
				writer.println(record.value().toString());
				writer.println(",");
			});
			for(int i=0;i<pointList.size()-1;i++)
			{
				writer.println(pointList.get(i).value().toString());
				writer.println(",");
			}
			writer.println(pointList.get(pointList.size()-1).value().toString());
			writer.println("]");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
