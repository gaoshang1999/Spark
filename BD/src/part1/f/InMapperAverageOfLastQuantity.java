package part1.f;



        
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import common.PairWritable;
        
public class InMapperAverageOfLastQuantity {
        
 public static class AvgInMapper extends Mapper<LongWritable, Text, Text, PairWritable> {
//    private final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
        
    private Map<String, PairWritable> map ;
    
    @Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, PairWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		for( Entry<String, PairWritable> entry: map.entrySet()){ 
			context.write(new Text(entry.getKey()), entry.getValue());
		}
	}


	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, PairWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		map = new HashMap<>();
	}
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] array = line.split(" ");
        String key2 = array[0];
        
        String v = array[array.length-1];
        if(v.matches("\\d+")){        
	        Integer value2 = Integer.parseInt(v);	        
//	        context.write(new Text(key2), new IntWritable(value2));
            if(!map.containsKey(key2)){
            	map.put(key2, new PairWritable(value2, 1));
            }else{
            	PairWritable val = map.get(key2);
            	map.put(key2,  new PairWritable( val.getFirst() + value2, val.getSecond() + 1) );
            }
        }
    }
 } 
        
 public static class AvgReduce extends Reducer<Text, PairWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<PairWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        int cnt = 0;
        for (PairWritable val : values) {
            sum += val.getFirst();
            cnt += val.getSecond();
        }
        context.write(key, new DoubleWritable(sum*1.0/cnt));
    }
 }
 
 
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "AverageOfLastQuantity");
        job.setJarByClass(InMapperAverageOfLastQuantity.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairWritable.class);
        
    job.setMapperClass(AvgInMapper.class);
    job.setReducerClass(AvgReduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
 
 
        
}