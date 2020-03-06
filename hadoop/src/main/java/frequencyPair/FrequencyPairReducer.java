package frequencyPair;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyPairReducer extends
	Reducer<Pair, IntWritable, Pair, DoubleWritable> {

	IntWritable Sum;

	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		super.setup(context);
		Sum = new IntWritable(0);
	}

	public void reduce(Pair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		Double sum = 0.0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		if (key.V.equals(new Text("*")))
			Sum = new IntWritable(sum.intValue());
		else
			context.write(key, new DoubleWritable(sum / Sum.get()));
	}
}
