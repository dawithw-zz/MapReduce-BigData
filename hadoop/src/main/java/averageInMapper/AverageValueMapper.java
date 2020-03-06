package averageInMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageValueMapper extends
	Mapper<LongWritable, Text, Text, CountAndValue> {
	
	Map<Text,CountAndValue> keyValue = new HashMap<>();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		Text ip = getIP(line);
		if (ip != null) {
			Integer packetSize = getPacketSize(line);
			if (packetSize != null) {
				if(keyValue.containsKey(ip)) {
					keyValue.get(ip).addValue(packetSize);
				} else {
					keyValue.put(ip, new CountAndValue(packetSize));
				}
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for(Text key : keyValue.keySet()) {
			context.write(key, keyValue.get(key));
		}
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



	private Text getIP(String line) {
		String[] lineParts = line.split(" ");
		String ipAddress = lineParts[0];
		if(isValidIp(ipAddress))
			return new Text(ipAddress);
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
				if(ipPartValue < 0 || ipPartValue > 255) {
					return false;
				}
					
			} catch (NumberFormatException ex) {
				return false;
			}
		}
		return true;
	}
}
