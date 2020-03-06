package wordCount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text word = new Text();
	private java.util.Map<Text,IntWritable> wordMap = new java.util.HashMap<>();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			IntWritable val = wordMap.get(word);
			if (val != null)
				val.set(val.get() + 1);
			else
				val = new IntWritable(1);
			wordMap.put(new Text(word),val);
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for(Text mapKey : wordMap.keySet()){
			System.out.println(mapKey + " -- " + wordMap.get(mapKey));
			context.write(mapKey,wordMap.get(mapKey));
		}
	}
}
