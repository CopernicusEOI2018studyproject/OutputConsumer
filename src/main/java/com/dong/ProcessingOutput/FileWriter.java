package com.dong.ProcessingOutput;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.notgroupb.formats.OutputDataPoint;


public class FileWriter {
	//output the list
	public void fileWriter()
	{
		
	}
	public void write(List<ConsumerRecord> pointList,List<OutputDataPoint> hour8pointList, String path)
	{
		// need to place the old files.
		try {
			if(pointList.size() != 0 && hour8pointList.size() != 0)
			{
				PrintWriter writer = new PrintWriter(path, "UTF-8");
				writer.println("[");
				
				for(int i=0;i<pointList.size()-1;i++)
				{
					writer.println(pointList.get(i).value().toString());
					writer.println(",");
				}
				writer.println(pointList.get(pointList.size()-1).value().toString());
				writer.println("],");
				//for 8hours
				writer.println("[");
				for(int i=0;i<hour8pointList.size()-1;i++)
				{
					writer.println(hour8pointList.get(i).toString());
					writer.println(",");
				}
				writer.println(hour8pointList.get(hour8pointList.size()-1).toString());
				writer.println("]");
				writer.close();
				
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public void writePoint(List<OutputDataPoint> hour8pointList, String path)
	{
		// need to place the old files.
				try {
					if(hour8pointList.size() != 0)
					{
						PrintWriter writer = new PrintWriter(path, "UTF-8");
						writer.println("[");
						
						for(int i=0;i<hour8pointList.size()-1;i++)
						{
							writer.println(hour8pointList.get(i).toString());
							writer.println(",");
						}
						writer.println(hour8pointList.get(hour8pointList.size()-1).toString());
						writer.println("]");
						writer.close();
					}
					
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
}
