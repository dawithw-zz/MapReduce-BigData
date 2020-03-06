package averageInMapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageValueReducer extends
	Reducer<Text, CountAndValue, Text, IntWritable> {
	
	@Override
	public void reduce(Text key, Iterable<CountAndValue> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		for (CountAndValue val : values) {
			sum += val.getValue();
			count += val.getCount();
		}
		int avg = sum/count;
		context.write(key, new IntWritable(avg));
	}
}
