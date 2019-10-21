package FB;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FacebookReducer  extends Reducer<Text, Text, Text, Text>
{
	//Key and list of values
	//Example: Ecommerce    [ { HuaHin,39,13 }  {Hua Hin,281,5 } {Bangkok,341,9} ............]
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context c)throws IOException, java.lang.InterruptedException
    {

    	// key: city  value: total_success_rate, count
	HashMap<String, String> cityData = new HashMap<String, String>();  // 1st Iteration -> [{HuaHin: 33.3,1}]
																	   // 2nd Iteration -> [{HuaHin: 35.07,2}]
	
	Iterator<Text> itr = values.iterator();   // point to the start of values list for each iteration
	
	while (itr.hasNext()) // While it still has value to process
	{
		//Suppose this is 2nd iteration
	    String f = itr.next().toString();       // f =  HuaHin,281,5

	    String[] words = f.split(",");                 //   words = [ {HuaHin} {281} {5}]
	    
	    String location = words[0].trim();            // location = HuaHin
	    
	    int clickCount = Integer.parseInt(words[1]);   // clickCount= 281
	    
	    int conversionCount = Integer.parseInt(words[2]);   //  conversionCount = 5 
	    
	    Double succRate = new Double(conversionCount/(clickCount*1.0)*100);   // succRate = 5 / 281 * 100 = 1.77

	    if (cityData.containsKey(location))
	    {
	    String s1 = cityData.get(location);  // s1 =  33.3,1
		String[] hValues = s1.split(",");         // hValues = [ {33.3} {1} ]            
		Double totalSuccRate = Double.parseDouble(hValues[0]) + succRate;    // totalSuccRate  = 33.3 + 1.77 = 35.07 
		int totalCount = Integer.parseInt(hValues[1]) + 1;             // totalCount =  2

		cityData.put(location, totalSuccRate + "," + totalCount);

	    }else
	    {
		cityData.put(location, succRate + ",1");
	    }
	}
            System.out.println(cityData.toString());
	for (Map.Entry<String, String> e : cityData.entrySet())  //HuaHin final value: e = HuaHin  value  65.07,3
	{
	    String[] V1 = e.getValue().split(",");        // V1  [{65.07} {3}]
	    Double avgSccRate = Double.parseDouble(V1[0])/Integer.parseInt(V1[1]);   //  avgSccRate = 65.07 / 3
	    c.write(key, new Text(e.getKey() + "," + avgSccRate));
	}
    }
}
