


        
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        
public class AverageOfLastQuantity {
        
 public static class AvgMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//    private final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] array = line.split(" ");
        String key2 = array[0];
        
        String v = array[array.length-1];
        if(v.matches("\\d+")){        
	        Integer value2 = Integer.parseInt(v);	        
	        context.write(new Text(key2), new IntWritable(value2));
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        int cnt = 0;
        for (IntWritable val : values) {
            sum += val.get();
            cnt += 1;
        }
        context.write(key, new DoubleWritable(sum*1.0/cnt));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "AverageOfLastQuantity");
        job.setJarByClass(AverageOfLastQuantity.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(AvgMapper.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}