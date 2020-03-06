package frequencyPair;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyPairMapper extends
	Mapper<LongWritable, Text, Pair, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] events = line.split(" ");

		for (int i = 0; i < events.length - 1; i++){
			String event = events[i];
			int j = i + 1;
			String next = events[j];
			while (!event.equals(next)) {
				context.write(new Pair(new Text(event), new Text(next)), one);
				context.write(new Pair(event, "*"), one);
				j++;
				if (j >= events.length)
					break;
				next = events[j];
			}
		}
	}
}
