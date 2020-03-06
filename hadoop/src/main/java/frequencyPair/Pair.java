package frequencyPair;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair implements Writable, WritableComparable<Pair> {
	public Text K, V;

	public Pair() {
		K = new Text();
		V = new Text();
	}

	public Pair(String e, String s) {
		K = new Text();
		K.set(e);
		V = new Text();
		V.set(s);
	}

	public Text getK() {
		return K;
	}

	public void setK(Text k) {
		K = k;
	}

	public Text getV() {
		return V;
	}

	public void setV(Text v) {
		V = v;
	}

	public Pair(Text k, Text v) {
		K = k;
		V = v;
	}

	@Override
	public boolean equals(Object obj) {
		return K.toString().equals(((Pair) obj).K.toString())
				&& V.toString().equals(((Pair) obj).V.toString());
	}

	@Override
	public String toString() {
		return String.format("(%s,%s)", K, V);
	}

	@Override
	public int compareTo(Pair o) {
		int comp = K.compareTo(o.K);
		if (comp != 0)
			return comp;
		else
			return V.compareTo(o.V);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {

		K.write(dataOutput);
		V.write(dataOutput);
	}

	@Override
	public int hashCode() {
		return K.toString().hashCode() + V.toString().hashCode();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		if (dataInput == null)
			return;
		K.readFields(dataInput);
		V.readFields(dataInput);
	}
}