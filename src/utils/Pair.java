package utils;

@SuppressWarnings("rawtypes")
public class Pair implements Comparable {
	public int x, y;

	public Pair(int x, int y) {
		this.x = x;
		this.y = y;
	}

	@Override
	public int compareTo(Object other) {
		if (x != ((Pair) other).x)
			return this.x - ((Pair) other).x;
		else
			return this.y - ((Pair) other).y;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		final Pair other = (Pair) obj;
		if (this.x == other.x && this.y == other.y) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return x *31 + y;
	}
	
	public String toString() {
		return String.format("(%d,%d)", x,y);
	}
}