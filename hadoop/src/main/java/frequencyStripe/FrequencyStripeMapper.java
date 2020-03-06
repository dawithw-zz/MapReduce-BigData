package frequencyStripe;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyStripeMapper extends 
	Mapper<LongWritable, Text, Text, MapWritable> {
		private static final IntWritable ONE = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
        	String line = value.toString();
            String[] ids = line.split(" ");
          
            for(int i = 0; i < ids.length; ++i) {
            	MapWritable map = new MapWritable();
            	String id = ids[i];
            	for(int j = i+1; j < ids.length; ++j) {
            		String neighbor = ids[j];
            		if(id.equals(neighbor))
            			break;
            		Text neighborText = new Text(neighbor);
            		if(map.get(neighborText) != null) {
            			IntWritable oldVal = (IntWritable)map.get(neighborText);
            			IntWritable newVal = new IntWritable(oldVal.get() + 1);
            			map.put(neighborText, newVal);
            		} else {
            			map.put(neighborText,ONE);
            		}
            	}
            	if (map.size() != 0) {
            		context.write(new Text(id), map);
            		System.out.println(id + " " +  string(map));
            	}
            }
            System.out.println("------END OF MAPPER -------");
        }
        public String string(MapWritable map) {
        	String s = "";
        	for(Writable key : map.keySet()) {
        		s += "( " + key + "," + map.get(key) + ") ";
        	}
        	return s;
        }
}
