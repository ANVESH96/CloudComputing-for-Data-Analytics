//Anvesh Mekala amekala@uncc.edu
//TFIDF Program 
// import the necessary libraries from hadoop
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

//the first map and reduce task compute the term frequency value of the words.
public class TfIdf {

	 public static class TokenizerMapper                   //this task has two maps and two reducers. So name of respective maps and reduces should be different
     extends Mapper<Object, Text,Text, IntWritable>{

  private final static IntWritable one = new IntWritable(1);
  private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
  	  String line = value.toString();
  	  String delimeter = "#####";                           //add delimeter to the key i.e.,word
        String filename = ( (FileSplit) context.getInputSplit()).getPath().getName().toString();     //this will get the filename which is our input
        for (String word : WORD_BOUNDARY.split(line)) {
          if (word.isEmpty()) {
              continue;
          }
             String currentword = (word+delimeter+filename).toLowerCase();                           //add delimeter and filename to the word i.e.,key which is to be passed to the reducer
              context.write(new Text(currentword),one);                                             //this method writes the outputkey and outputvalue which are intermediate values in mapreduce
          }
      }
}

  public static class IntSumReducer                   //input key and value of IntSumReducer should match with output key ,value pair of TokenizerMapper
       extends Reducer<Text,IntWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();                       //sum of all the counts for each key
      }
      double termfrequency = 0.00000000;
      
       termfrequency = 1+Math.log10(sum);      //calculate term frequncy value and store it as doublevalue
      result.set(termfrequency);               //set the result to termfrequency
      context.write(key, result);              //write the output key and value from the reducer
    }
  }
//this map reduce task gets the TFIDF values for a word and its respective appears in files.
  public static class Mapper2
  extends Mapper<Object,Text,Text, Text>{          //here the object and Text which are input key and values were retreived from Intsumreducer
	public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
  String newword = value.toString();                
  String keyword[] = newword.split("#####");      //split the word using the delimeter
  Text keywordfinal = new Text(keyword[0]);       // the keywordfinal is the key to be parsed to the reducer
  String valueword1[] = keyword[1].split("\\s+"); // split the "filename  tfvalue" in to filename and tfvalues
  String valueword2 =valueword1[0]+"="+valueword1[1]; //add filename and tfvalues and place a equal operator between them
  Text valueword = new Text(valueword2);              //convert the string to Text 
  context.write(keywordfinal, valueword);
 }
}


  public static class Reducer2                    
  extends Reducer<Text,Text,Text,DoubleWritable> {       //input key and value of reducer2 should match outkey and value of maper2
	  private static String inputString;
public void reduce(Text key, Iterable<Text> values,
                  Context context
                  ) throws IOException, InterruptedException {
	Configuration conf2 = context.getConfiguration();
	inputString = conf2.get("Inputfilelength");             //read the input from user and store it to a string
	  double filelength = Double.parseDouble(inputString.toString());   //Convert the inputfiles lenght to double which can be used to calculate idf
 ArrayList<String> filevalues = new ArrayList<String>();     //arrayslist which can store files of a particular word appearance
 ArrayList<Double> tfvalues = new ArrayList<Double>();       //tf values of each files
 for (Text val : values) {                                   //iterate theough each value 
   String files = val.toString();
   String filesrecord[] = files.split("=");                  //split value using equals operator as split boundary
   tfvalues.add(Double.parseDouble(filesrecord[1]));         //tfvalues would be the first index of filesrecord
   filevalues.add(filesrecord[0]);                           //filevalues would be the zeroth index of filesrecord
 }
 double idf = Math.log10(1+(filelength/(filevalues.size()))); //calcualte idf values
// ArrayList<Double> tfidf = new ArrayList<Double>();
 for (int i=0;i<tfvalues.size();i++){                        //run loop through each filename and tf values 
	 double tfidf=(idf*tfvalues.get(i));                     //calculate corresponfing tfidf values for each file name
	 String keyvalue = key.toString()+"#####"+filevalues.get(i); //create key value with delimeter and filename
	 Text keytext = new Text(keyvalue);
	 context.write(keytext,new DoubleWritable(tfidf));	 //write the ouput key and vales 
 }
 
}
}

  public static void main(String[] args) throws Exception {
	JobControl jobControl = new JobControl("Chainned Job");   //setup job controller to manage the execution of jobs
    Configuration conf1 = new Configuration();                  
    Job job = Job.getInstance(conf1, "tf");                   //this executes the mapreduce task which compute termfrequency
    job.setJarByClass(TfIdf.class);                           
    job.setMapperClass(TokenizerMapper.class);                //this refers to mapclass of termfrequency job
    job.setReducerClass(IntSumReducer.class);                 //this referes to reduce class  of termfrequency job
    job.setOutputKeyClass(Text.class);                        //output key from the Tokenizermapper class
    job.setOutputValueClass(IntWritable.class);               //output value from Tokenizermappper class
    FileInputFormat.addInputPath(job, new Path(args[0]));     //arg statement which gets the inputfolder path  for job
    FileOutputFormat.setOutputPath(job, new Path(args[1]));   //arg statement which gets the input folder path for job
    
    ControlledJob controlledJob1 = new ControlledJob(conf1);  
    controlledJob1.setJob(job);                               //controlled job is implemented on job "which is our first mapreduce job"
    jobControl.addJob(controlledJob1);                        //add job to the job controller 
    
    Configuration conf2 = new Configuration();                
    conf2.set("Inputfilelength",args[3]);                     //we configure the userinput here which can be used in map2 or reduce2 and the args index of it
    Job job2 = Job.getInstance(conf2,"Tfidf");                //this executes the mapreduce task which compute TFIDF
    job2.setJarByClass(TfIdf.class);
    job2.setMapperClass(Mapper2.class);                       //this refers to mapclass of termfrequency job2
    job2.setReducerClass(Reducer2.class);                     //this referes to reduce class  of termfrequency job2
    job2.setOutputKeyClass(Text.class);                       //output key from the map2
    job2.setOutputValueClass(Text.class);                     //output value from map2
  
    FileInputFormat.addInputPath(job2, new Path(args[1]));    //arg statement which gets the inputfolder path  for job2
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));  //arg statement which gets the input folder path for job
    
    ControlledJob controlledJob2 = new ControlledJob(conf2);  //setup controlled job which executes the jobs in sequence
    controlledJob2.setJob(job2);                              //append job2 to the controlled job
    
    controlledJob2.addDependingJob(controlledJob1);           //set dependency of one controller job over other
    // add the job to the job control
    jobControl.addJob(controlledJob2);                        //add job2 to the jobcontrol
    Thread jobControlThread = new Thread(jobControl);         //create a thread which holds the sequence of process
    jobControlThread.start();                                 //initiaze the thread
    
while (!jobControl.allFinished()) {                           //this whileloop return the joblist status 
    System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());   //get the number of waitinglob
    System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());      //get the number of joblist 
    System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());   //get the  number of running joblist number
    System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());  //get the sucessful job list
    System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());       //get the  number offailured job lists.
try {
    Thread.sleep(5000);                                                                        //set the sleep mode for thread with milliseconds 
    } catch (Exception e) {                                                                    //catch the errors in thread
    }

  } 
  System.exit(job2.waitForCompletion(true) ? 0 : 1);                                           //exit when job2 is completed.
  }   
   
}


	

