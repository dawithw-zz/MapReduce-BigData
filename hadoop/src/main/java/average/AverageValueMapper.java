package average;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageValueMapper extends
	Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String ipString = getIP(line);
		Integer packetSize = getPacketSize(line);	
		if(ipString != null && packetSize != null)
			context.write(new Text(ipString), new IntWritable(packetSize));
	}
	
	private Integer getPacketSize(String line) {
		String[] lineParts = line.split(" ");
		if (lineParts.length < 2)
			return null;
		String lastValue = lineParts[lineParts.length-1];
		try {
			return Integer.parseInt(lastValue);
		} catch (NumberFormatException ex) {
			return null;
		}
	}

	private String getIP(String line) {
		String[] lineParts = line.split(" ");
		String ipAddress = lineParts[0];
		if(isValidIp(ipAddress))
			return ipAddress;
		return null;
	}

	private boolean isValidIp(String ipAddress) {
		String[] ipParts = ipAddress.split("\\.");
		if(ipParts.length != 4) {
			return false;
		}
		for(String ipPart : ipParts) {
			try {
				int ipPartValue = Integer.parseInt(ipPart);
				if(ipPartValue < 0 || ipPartValue > 255) 
					return false;
			} catch (NumberFormatException ex) {
				return false;
			}
		}
		return true;
	}
}
