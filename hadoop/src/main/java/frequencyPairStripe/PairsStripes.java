package frequencyPairStripe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class PairsStripes {

    public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] Events = line.split(" ");

            for(int i=0;i<Events.length-1;i++)
            {   
            	String E=Events[i];
                 int j=i+1;
                 String next=Events[j];

                while (!E.equals(next) )
                {
                    context.write(new Pair(new Text(E),new Text(next)),one);
                    System.out.println("(" + E + "," + next + ")");
                    j++;
                    if (j>= Events.length) break;
                    next=Events[j];
                }
            }
        }
    }



    public static class Reduce extends Reducer<Pair, IntWritable, Text, Text> {

        IntWritable Sum ;
        HashMap<String,HashMap<String,Double>> map;

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            for (java.util.Map.Entry<String, HashMap<String,Double>> s:map.entrySet()) {
            	String key = s.getKey();
            	HashMap<String,Double> keyValue = s.getValue();
            	HashMap<String,Double> keyValueAverage = new HashMap<>();
            	
            	int sum = 0;
            	for(java.util.Map.Entry<String,Double> entry : keyValue.entrySet()) {
            		sum += entry.getValue();
            	}
            	
            	// map count to average
            	for(java.util.Map.Entry<String,Double> entry : keyValue.entrySet()) {
            		keyValueAverage.put(entry.getKey(), entry.getValue()/sum);
            	}
            	
            	String val = stringify(keyValueAverage);
            	context.write(new Text(key), new Text(val));
            }
        }
        
        private String stringify(HashMap<String, Double> keyValue) {
			String ret = "[ ";
			for(java.util.Map.Entry<String,Double> entry : keyValue.entrySet()) {
				ret += "(" + entry.getKey() + "," + String.format("%.2f",entry.getValue()) + ") ";
			}
			return ret + "]";
		}

		@Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Sum=new IntWritable(0);
            map = new HashMap<>();
        }

        public void reduce(Pair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            Double sum = 0.0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            String mainKey = key.getK().toString();
            String secondaryKey = key.getV().toString();
            
            if (map.containsKey(mainKey)) {
            	HashMap<String,Double> innerMap = map.get(mainKey);
            	if(innerMap.containsKey(secondaryKey))
            		innerMap.put(secondaryKey,(Double)innerMap.get(secondaryKey) + sum);
            	else
            		innerMap.put(secondaryKey,sum);
            } else {
            	HashMap<String,Double> internalKeyValue = new HashMap<>();
            	internalKeyValue.put(secondaryKey, sum);
            	map.put(mainKey, internalKeyValue);
            }
        }
    }



    public static void run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "wordcount");

        job.setJarByClass(PairsStripes.class   );

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(appendInstance(args[1])));

		job.waitForCompletion(true);
	}
	
	private static String appendInstance(String folder) {
		DateFormat dateFormat = new SimpleDateFormat("_yyyy.MM.dd_HH.mm.ss");
		Date date = new Date();
		return folder + dateFormat.format(date);
	}


}