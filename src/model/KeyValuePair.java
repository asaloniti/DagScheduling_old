package model;

public class KeyValuePair implements Comparable<KeyValuePair> {
	public String key;
	public int value;

	public KeyValuePair(String k, int v) {
		key = k;
		value = v;
	}

	@Override
	public int compareTo(KeyValuePair arg0) {
		int value = arg0.value;
		return this.value - value;
	}

}
