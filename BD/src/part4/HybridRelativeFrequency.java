package part4;



        
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import common.MyMapWritable;
import common.Neighbors;
import common.PairPartitioner;
import common.PairWritableComparable;
        
public class HybridRelativeFrequency {
        
 public static class Map extends Mapper<LongWritable, Text, PairWritableComparable, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] items = value.toString().split(" |\t");
        for( int i=0; i< items.length; i++){
        	String w = items[i];
        	for(String u : Neighbors.neighbors(items, i)){
        		System.out.println("(" + w +","+u +")");
        		context.write(new PairWritableComparable(new Text(w), new Text(u)), one);        		
        	}        	
        }
    }
 } 
        
 public static class Reduce extends Reducer<PairWritableComparable, IntWritable, Text, MyMapWritable> {
	
	 private int sumByGroup = 0; 
	 private MyMapWritable map ;
	 private Text preKey ;
	 
	 

	@Override
	protected void cleanup(
			Reducer<PairWritableComparable, IntWritable, Text, MyMapWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		computeAndEmit(context);
	}



	@Override
	protected void setup(
			Reducer<PairWritableComparable, IntWritable, Text, MyMapWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		map = new MyMapWritable();
		preKey = null;
	}


	private void computeAndEmit(Context context) throws IOException, InterruptedException { 
		int sum = 0;
   	 
		for( Entry<Writable, Writable> e: map.entrySet()){
        	IntWritable v= (IntWritable)e.getValue();
        	sum += v.get();
        }
    	
		for( Entry<Writable, Writable> e: map.entrySet()){
        	IntWritable v= (IntWritable)e.getValue();
        	map.put( e.getKey() , new DoubleWritable(v.get() * 1.0 / sum));
        }
    	
    	context.write(preKey, map);
	}

	@Override
    public void reduce(PairWritableComparable key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {

        if(!key.getFirst().equals(preKey) && preKey != null){ // key changed 
        	computeAndEmit(context);
        	
        	map.clear();
        }
        
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
            System.out.println("(" + key +","+ val +")");
        }
        
        map.put(new Text(key.getSecond()), new IntWritable(sum));
        System.out.println( map);
        System.out.println( );
        preKey = new Text(key.getFirst());
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "HybridRelativeFrequency");
        job.setJarByClass(HybridRelativeFrequency.class);
        
        job.setNumReduceTasks(2);

    job.setMapOutputKeyClass(PairWritableComparable.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    
    job.setPartitionerClass(PairPartitioner.class);
       
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}