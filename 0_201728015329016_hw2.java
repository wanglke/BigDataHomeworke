/**
 * @file Hw2Part1.java
 * @author  201728015329016
 * @version 0.1
 * 2018/05/01
 */
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.math.*;
import java.lang.*;
import org.apache.hadoop.io.*;

public class Hw2Part1 {

  // This is the Mapper class
  // reference: http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Mapper.html
  //
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	private Text mapout = new Text();
	   
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
						String line = value.toString();
						String[] fields = line.split("\\s+");
						if(fields.length==3){
							word.set(fields[0]+" "+fields[1]);
							mapout.set(fields[2]);
							context.write(word, mapout);
						}
						
      //StringTokenizer itr = new StringTokenizer(value.toString());
      // while (itr.hasMoreTokens()) {
        // word.set(itr.nextToken());
        // context.write(word, one);
      // }
    }
  }
  

  
  // this is the reducer class
  // reference http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/reducer.html
  //
  // we want to control the output format to look at the following:
  //
  // count of word = count
  //
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text result_key= new Text();
    private Text result_value= new Text();
    private byte[] prefix;
    private byte[] suffix;

    protected void setup(Context context) {
      try {
        prefix= Text.encode("").array();
        suffix= Text.encode("").array();
      } catch (Exception e) {
        prefix = suffix = new byte[0];
      }
    }

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int count = 0;
	  BigDecimal sum = new BigDecimal(0);
      for (Text val : values) {
        BigDecimal bd = new BigDecimal(val.toString());
		sum = sum.add(bd);
		count++;
      }

	    BigDecimal number = new BigDecimal(0);
	    number=BigDecimal.valueOf((int)count);
	  	
		BigDecimal average = sum.divide(number,3,RoundingMode.HALF_UP);
		String value=""+count+" "+average;
		
		result_key.set(prefix);
		result_key.append(key.getBytes(), 0, key.getLength());
		result_key.append(suffix, 0, suffix.length);
		result_value.set(value);
		context.write(result_key, result_value);
    }
  }

  public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 String[] otherArgs = new GenericOptionsParser(conf, 
args).getRemainingArgs();
 if (otherArgs.length < 2) {
 System.err.println("Usage: wordcount <in> [<in>...] <out>");
 System.exit(2);
 }
 Job job = Job.getInstance(conf, "Hw2Part1");
 job.setJarByClass(Hw2Part1.class);
 //job.setJarByClass(WordCount.class);
 job.setMapperClass(TokenizerMapper.class);
 job.setReducerClass(IntSumReducer.class);
 job.setMapOutputKeyClass(Text.class);
 job.setMapOutputValueClass(Text.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(Text.class);
 // add the input paths as given by command line
 for (int i = 0; i < otherArgs.length - 1; ++i) {
 FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
 }
 // add the output path as given by the command line
 FileOutputFormat.setOutputPath(job,
 new Path(otherArgs[otherArgs.length - 1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}
