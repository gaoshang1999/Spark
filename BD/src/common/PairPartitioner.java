package common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairPartitioner extends Partitioner<PairWritableComparable, IntWritable>{

	@Override
	public int getPartition(PairWritableComparable key, IntWritable value,
			int numPartitions) {
		// TODO Auto-generated method stub
		
		return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
