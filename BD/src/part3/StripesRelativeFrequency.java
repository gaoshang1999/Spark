package part3;



        
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
        
public class StripesRelativeFrequency {
        
 public static class Map extends Mapper<LongWritable, Text, Text, MyMapWritable> {
    private final static IntWritable one = new IntWritable(1);
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] items = value.toString().split(" ");
        for( int i=0; i< items.length; i++){
        	Text w = new Text(items[i]);
        	MyMapWritable map = new MyMapWritable();
        	for(String u : Neighbors.neighbors(items, i)){   
        		Text k = new Text(u);
        		if(!map.containsKey(k)){
        			map.put(k, one);
        		}else{
        			IntWritable v= (IntWritable)map.get(k);
        			map.put(k, new IntWritable(v.get()+1));
        		}
        		 
        	}        	
        	context.write(w, map);
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, MyMapWritable, Text, MyMapWritable> {
	
	@Override
    public void reduce(Text key, Iterable<MyMapWritable> values, Context context) 
      throws IOException, InterruptedException {
		MyMapWritable map = new MyMapWritable();
		int sum = 0;
        for (MyMapWritable val : values) {
             for(  Entry<Writable, Writable> e: val.entrySet()){
            	 Text k = (Text)e.getKey();    
            	 IntWritable v= (IntWritable)e.getValue();
            	 sum += v.get();
            	 if(!map.containsKey(k)){
         			map.put(k, v);
         		}else{
         			IntWritable oldV= (IntWritable)map.get(k);
         			map.put(k, new IntWritable(v.get() + oldV.get()));
         		}            	 
             }
        }
        
        for( Entry<Writable, Writable> e: map.entrySet()){
        	IntWritable v= (IntWritable)e.getValue();
        	map.put(e.getKey(), new DoubleWritable(v.get() * 1.0 / sum));
        }
       
        context.write(key, map);
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "StripesRelativeFrequency");
        job.setJarByClass(StripesRelativeFrequency.class);
        
        job.setNumReduceTasks(1);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MyMapWritable.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MyMapWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    
//    job.setGroupingComparatorClass(PairWritableComparator.class);
       
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}