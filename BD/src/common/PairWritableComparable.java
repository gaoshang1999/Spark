package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PairWritableComparable implements WritableComparable<PairWritableComparable> {
	private Text first = new Text();
	private Text second = new Text();
		
	public PairWritableComparable() {
		super();
	}
	public PairWritableComparable(Text first, Text second) {
		super();
		this.first = first;
		this.second = second;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		this.first.write(out);
		this.second.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.first.readFields(in);
		this.second.readFields(in);
	}
	public Text getFirst() {
		return first;
	}
	public Text getSecond() {
		return second;
	}
	@Override
	public int compareTo(PairWritableComparable o) {
		int dif = this.first.compareTo(o.first);
		if(dif != 0){
			return dif;
		}		
		if(this.second.equals(STAR_4_Order_Inversion) && ! o.second.equals(STAR_4_Order_Inversion)){
			return -1;
		}
		return this.second.compareTo(o.second);
	}	 
	
	public static final Text STAR_4_Order_Inversion = new Text("*");

	@Override
	public String toString() {
		return "(" + first + ", " + second + ")";
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PairWritableComparable other = (PairWritableComparable) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	} 
	
	
	
 }
