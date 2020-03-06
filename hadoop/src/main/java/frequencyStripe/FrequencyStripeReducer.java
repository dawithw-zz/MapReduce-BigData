package frequencyStripe;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyStripeReducer extends 
	Reducer<Text, MapWritable, Text, Text> {

    public void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {

    	int totalCount = 0;
        MapWritable valueCounter = new MapWritable();
        
        for(MapWritable map : values) {
        	for(MapWritable.Entry entry : map.entrySet()) {
        		Text k = (Text)entry.getKey();
        		IntWritable v = (IntWritable)entry.getValue();
        		totalCount += v.get();
        		if(valueCounter.containsKey(k)) {
        			IntWritable existingValue = (IntWritable)valueCounter.get(k);
        			valueCounter.put(k, new IntWritable(v.get() + existingValue.get()));
        		} else {
        			valueCounter.put(k, v);
        		}
        	}
        }
        
        for(MapWritable.Entry entry : valueCounter.entrySet()) {
        	double avg = ((IntWritable)entry.getValue()).get()*1.0/totalCount;
        	valueCounter.put((Text)entry.getKey(), new DoubleWritable(avg));
        }
        
        String valueString = "[ " + stringify(valueCounter) + "]";
        context.write(key, new Text(valueString));
        System.out.println(key + "\t" + (valueCounter.entrySet().size() > 0 ? valueString : "empty"));
    }	

	private String stringify(MapWritable mw) {
		String vals = "";
		for(MapWritable.Entry entry : mw.entrySet()) {
			double avg = ((DoubleWritable)entry.getValue()).get();
			vals += "( " + entry.getKey() + "," + String.format("%.2f",avg) + ") ";
		}
		return vals;
	}
}
