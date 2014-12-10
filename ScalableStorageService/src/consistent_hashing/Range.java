package consistent_hashing;

public class Range implements  java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -1695489559108165310L;
	private int low;
	private int high;

	public Range(int low, int high) {
		this.low = low;
		this.high = high;
	}

	public int getLow() {
		return this.low;
	}

	public int getHigh() {
		return this.high;
	}

	public void setLow(int value) {
		this.low = value;
	}

	public void setHigh(int value) {
		this.high = value;
	}

	public boolean isWithin(int hash) {
		if(this.low < this.high){
			if (this.low < hash && this.high >= hash)
				return true;
		}else if(this.low==this.high){
			return true;
		}
		else{
			if (this.low < hash || this.high >= hash)
				return true;
		}
		return false;
	}

}
