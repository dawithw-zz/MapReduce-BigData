package average;

public class CountAndValue {
	private int value;
	private int count;
	
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
}
