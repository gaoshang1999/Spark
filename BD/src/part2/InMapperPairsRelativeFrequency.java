package part2;



        
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import common.Neighbors;
import common.PairPartitioner;
import common.PairWritable;
import common.PairWritableComparable;
        
public class InMapperPairsRelativeFrequency {
        
 public static class Map extends Mapper<LongWritable, Text, PairWritableComparable, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    
    private java.util.Map<PairWritableComparable, IntWritable> map ;
    
    @Override
	protected void cleanup(
			Mapper<LongWritable, Text, PairWritableComparable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		for( Entry<PairWritableComparable, IntWritable> e : map.entrySet()){
			context.write(e.getKey(), e.getValue());
		}
	}


	@Override
	protected void setup(
			Mapper<LongWritable, Text, PairWritableComparable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		map = new HashMap<>();
	}


	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] items = value.toString().split(" |\t");
        for( int i=0; i< items.length; i++){
        	String w = items[i];
        	for(String u : Neighbors.neighbors(items, i)){
        		System.out.println("(" + w +","+u +")");
//        		context.write(new PairWritableComparable(new Text(w), new Text(u)), one);
//        		context.write(new PairWritableComparable(new Text(w), PairWritableComparable.STAR_4_Order_Inversion), one);
        		
        		PairWritableComparable key1 = new PairWritableComparable(new Text(w), new Text(u));
        		if(!map.containsKey(key1)){
                 	map.put(key1, one);
                 }else{
                	IntWritable val = map.get(key1);
                 	map.put(key1,  new IntWritable( val.get() + 1) );
                 }
        		
        		PairWritableComparable key2 = new PairWritableComparable(new Text(w), PairWritableComparable.STAR_4_Order_Inversion);
        		if(!map.containsKey(key2)){
                 	map.put(key2, one);
                 }else{
                	IntWritable val = map.get(key2);
                 	map.put(key2,  new IntWritable( val.get() + 1) );
                 }        		
        	}        	
        }
    }
 } 
        
 public static class Reduce extends Reducer<PairWritableComparable, IntWritable, PairWritableComparable, DoubleWritable> {
	
	 private int sumByGroup = 0; 

	@Override
    public void reduce(PairWritableComparable key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
		 
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
            System.out.println("(" + key +","+ val +")");
        }
        System.out.println( );

        if(key.getSecond().equals(PairWritableComparable.STAR_4_Order_Inversion)){
        	sumByGroup = sum;
        }else{
        	context.write(key, new DoubleWritable(sum*1.0/sumByGroup));
        }
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(InMapperPairsRelativeFrequency.class);
        
        job.setNumReduceTasks(2);

    job.setMapOutputKeyClass(PairWritableComparable.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(PairWritableComparable.class);
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