//Anvesh Mekala amekala@uncc.edu
//Search Program 
//import the necessary libraries from hadoop
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount.TokenizerMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Search {
//this mapreduce task consider output of tfidf java file as input
  public static class TokenizerMapper
       extends Mapper<Object, Text,Text, DoubleWritable>{
	  private static String inputString;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();
    	inputString = conf.get("searchquery");                 //get the inputquery string of the user and assign itto a string
    	String inputquery =(inputString.toLowerCase());        //convert the user input query to lowercase to avoid case sensitive retreivals
    	String inputsplit[] = inputquery.split("\\s+");        //split the input query by space boundary
    String valueline = value.toString();                       
    String keyvalues[] = valueline.split("#####");             //split inputvalue using delimeter as split boundary
    String wordinline = keyvalues[0];                          //store words in to a array
    String wordline2 =keyvalues[1];
    String filename[] = wordline2.split("\\s+");              //split the string by space
    Text keyterm = new Text(filename[0]);                     //get the filenames into a Text
    double valterm = Double.parseDouble(filename[1]); 	      //store tfidf values 
    DoubleWritable one = new DoubleWritable(valterm);         
    int i=0;
    for(i=0;i<(inputsplit.length);i++){                       //perform search operation for all the tokens present in the input query string
    	if(inputsplit[i].equalsIgnoreCase(wordinline)){       
    		context.write(keyterm,one);                       //write as intermediate output which has key and tfidfvalues of the respective files
    	}
    }
 
    }
  }

  public static class reducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {  //input key and value of reducer should match outkey and value of maper
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      Double sum = 0.0;                                           
      for (DoubleWritable val : values) {                         //compute sum of all tfidf values of respective files for a given inputquery
        sum += val.get();
      }
      result.set(sum);     
      context.write(key, result);                                 
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("searchquery",args[2]);                           //configure the input serch query 
    Job job = Job.getInstance(conf, "word count");     
    job.setJarByClass(Search.class);                           
    job.setMapperClass(TokenizerMapper.class);                //set map class
    job.setCombinerClass(reducer.class);                      //set combiner class which is reducer itself
    job.setReducerClass(reducer.class);                       //set reducer class
    job.setOutputKeyClass(Text.class);                        //set the output key class of mapper
    job.setOutputValueClass(DoubleWritable.class);            //set the outputvalue class of mapper
    FileInputFormat.addInputPath(job, new Path(args[0]));     //args index to parse the input folder path
    FileOutputFormat.setOutputPath(job, new Path(args[1]));   //args index to parse the output folder path
    System.exit(job.waitForCompletion(true) ? 0 : 1);         //exit when job is done
  }
}

