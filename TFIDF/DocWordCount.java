//Anvesh Mekala amekala@uncc.edu
//DocWordCount Program 
// import the necessary libraries from hadoop
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text,Text, IntWritable>{              

    private final static IntWritable one = new IntWritable(1);    //initializing the value to int 1
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");    //tokeniation using pattern matching
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	  String line = value.toString(); 
    	  String delimeter = "#####";                            //add delimeter to the key i.e.,word
          String filename = ( (FileSplit) context.getInputSplit()).getPath().getName().toString();         //this will get the filename which is our input
          for (String word : WORD_BOUNDARY.split(line)) {
            if (word.isEmpty()) {
                continue;
            }
               String currentword = (word+delimeter+filename).toLowerCase();       //add delimeter and filename to the word i.e.,key which is to be passed to the reducer
                context.write(new Text(currentword),one);                          //this method writes the outputkey and outputvalue which are intermediate values in mapreduce
            }
        }
  }

  public static class IntSumReducer            //input key and value of IntSumReducer should match with output key ,value pair or TokenizerMapper
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;  
      for (IntWritable val : values) {
        sum += val.get();                        //sum of all the counts for each key
      }
      result.set(sum);
      context.write(key, result);                
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();   
    Job job = Job.getInstance(conf, "word count");   
    job.setJarByClass(WordCount.class);         
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);    // we append reducer as combiner class where we dont had any explicit combiner
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);            //setOutputKeyClass is the intermediate key parsed by mapper
    job.setOutputValueClass(IntWritable.class);   //setOutputKeyClass is the intermediate value parsed by mapper
    FileInputFormat.addInputPath(job, new Path(args[0])); //args define the index in which the execution command should be written
    FileOutputFormat.setOutputPath(job, new Path(args[1])); 
    System.exit(job.waitForCompletion(true) ? 0 : 1);      //exit when job is done.
  }
}
