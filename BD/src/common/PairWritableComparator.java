package common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PairWritableComparator extends WritableComparator {

	
	
	public PairWritableComparator() {
		super(PairWritableComparable.class, true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		PairWritableComparable p1 = (PairWritableComparable)a;
		PairWritableComparable p2 = (PairWritableComparable)b;
		return p1.getFirst().compareTo(p2.getFirst());
	}
	 
 }
