package FB;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

//Receive input (Example: FKLY490998LB,2018-01-29 06:12:17,HuaHin,Ecommerce,39,13,25-35)
public class FacebookMapper extends Mapper<LongWritable, Text, Text, Text>
{
	 
	    @Override
	    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
	    {

		String line = value.toString(); //store value in line
		String[] words = line.split(",");    // split the line into array of words
										    // [{FKLY490998LB} {2018-01-29 06:12:17} {Hua Hin} {Ecommerce} {39} {13} {25-35}]

		
		c.write(new Text(words[3]), new Text (words[2] + "," + words[4] + "," +  words[5])); // define key value pair
	    }     // Ecommerce                   HuaHin        ,   39          ,      13  
	}