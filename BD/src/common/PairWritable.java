package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PairWritable implements Writable {
	private int first;
	private int second;
		
	public PairWritable() {
		super();
	}
	public PairWritable(int first, int second) {
		super();
		this.first = first;
		this.second = second;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
	}
	public int getFirst() {
		return first;
	}
	public int getSecond() {
		return second;
	}	 
	
	
 }
