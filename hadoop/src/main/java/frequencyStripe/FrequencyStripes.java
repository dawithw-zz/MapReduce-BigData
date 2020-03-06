package frequencyStripe;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class FrequencyStripes {
	public static void run(String[] args) throws Exception {
		Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "Relative Frequency Stripes");
        job.setJarByClass(FrequencyStripes.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.setMapperClass(FrequencyStripeMapper.class);
        job.setReducerClass(FrequencyStripeReducer.class);

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
