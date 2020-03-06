package averageInMapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CountAndValue implements Writable {
	private int value;
	private int count;
	
	public CountAndValue() {
		count = 0;
		value = 0;
	}
	
	public CountAndValue(int value) {
		this.value = value;
		this.count = 1;
	}
	
	public void addValue(int value) {
		this.value += value;
		++count;
	}
	
	public int getCount() {
		return count;
	}
	
	public int getValue() {
		return value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readInt();
		count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(value);
		out.writeInt(count);
	}
}
