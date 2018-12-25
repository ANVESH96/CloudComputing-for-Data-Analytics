//import all the neccessary packages 
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Test_pr {
	//first mapreduce task which computes the no. of valid lines 
	public static class CountMapper
    extends Mapper<Object, Text,Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);  
public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
	String line = value.toString();
	//check for the titles and capture them using patter and matcher methods.
	Pattern pattern = Pattern.compile("<title>(.+?)</title>");
	  Matcher matcher = pattern.matcher(line);
	  /*write the title only if the match is found. 
	  This helps us handling empty lines or webpages which donot actually exist*/
	  if(matcher.find()){
		  context.write(new Text("Count"),one);
	  }
	     
}
}
//reducer function to compute the number of lines
public static class CountReducer
    extends Reducer<Text,IntWritable,Text,Text> {
	
     public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
    	 int count =0; 
    	 for(IntWritable val : values){
    		 count += val.get();
    	 }
    	context.write(key,new Text(Integer.toString(count)));
      	
     }   	
}

//Second Mapreduce function which computes initial pagerank
public static class InitialprMapper
       extends Mapper<Object, Text,Text, Text>{
	 	  
   public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	      String line = value.toString();
	      //find titles using pattern matching
    	  Pattern pattern = Pattern.compile("<title>(.+?)</title>");
    	  Matcher matcher = pattern.matcher(line);
    	  //find text tag onlyif titles are found
    	  if(matcher.find()){
    		  Text title = new Text(matcher.group(1));
    		   //find the whole string written inside the text tag
    		  Pattern pattern2 = Pattern.compile("<text(.*?)</text>");
    		  Matcher matcher2 = pattern2.matcher(line);
    		  //identify the outlinks only if text tags are found
    		  if(matcher2.find()){
    			  String text = matcher2.group(1);
    			  Pattern pattern3 = Pattern.compile("\\[\\[(.*?)\\]\\]");
    			  Matcher matcher3 = pattern3.matcher(text);
    		  while (matcher3.find()) {
    			  Text outlinks = new Text(matcher3.group(1));
    			  //write the title and their outlinks
    			  context.write(title,outlinks);               
    	            } 
    		  }
    	  }
  }
  }
   
  public static class InitialprReducer extends Reducer<Text,Text,Text,Text> {
	  double initialrank;
	  
        public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
        	//get the number of lines which is computed from CountMapper and CountReducer
        	Configuration conf = context.getConfiguration();
        	String numberoflines = conf.get("numberoflines");
        	//get the initial rank using 1/n 
            double initial_rank = 1.00/(Double.parseDouble(numberoflines));
            String initialrank = Double.toString(initial_rank);
            //define delimeter to differentiate Pageranka and outlinks
    	String delimeter = "#####";   
    	String delimeter2 = "$$$";
        StringBuffer hyperlink_withdelimeter = new StringBuffer();
    for (Text val : values) {
    	
    	//append the outlinks to the string buffer
    	hyperlink_withdelimeter.append((val.toString()+delimeter)); 	
    }
      String combined = (initialrank)+(delimeter2+(hyperlink_withdelimeter.toString()));
      Text result = new Text(combined);
      //write the title as key and pagerank, outlinks as value from the reducer
     context.write(key,result);
   }
  }

  /*this mapreduce task iterates the pageranks for 10 iterations which we assume they converge
  we get the input form the above mapreduce where we compute intial pagerank*/
  public static class SecondMapper
  extends Mapper<Object, Text,Text, Text>{
 
public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
        
			  String line = value.toString();
			  //split the text to get the key
         	  String[] title = line.split("\t");
         	  //assign the key to keytitle
         	  Text keytitle = new Text(title[0]);
         	  String delimeter = "#A#A";
         	  //write it to the reducer which helps us to verify whether title exists
         	  context.write(keytitle,new Text(delimeter));
	           String[] pagerank_outlinks = title[1].split("\\$\\$\\$");
	           String delimeter2 = "#$#$";
	           /*check whether there are any outlinks from the webpage by finding the array length of array which is
	           returned after splitting the outlinks and pagerank */
	           if(pagerank_outlinks.length == 1){
	        	   return;
	           }
	           //add delimeter to identify the outlinks in reducer phase
	           String outlinks_withdelimeter = delimeter2+pagerank_outlinks[1];
	           context.write(keytitle,new Text(outlinks_withdelimeter));
	           //store the pagerank 
	          double current_pagerank = Double.parseDouble(pagerank_outlinks[0]);
	          String[] outlinks = pagerank_outlinks[1].split("#####");
	          double num_outlinks = outlinks.length;
	          String intermediate_pagerank = Double.toString((current_pagerank)/(num_outlinks));
	          //compute the pagerank for outlinks and write it to reducer
	          for(int i =0; i <num_outlinks;i++){
	        	  context.write(new Text(outlinks[i]),new Text(intermediate_pagerank));
	          } 
}
}

public static class SecondReducer
  extends Reducer<Text,Text,Text,Text> {
   public void reduce(Text key, Iterable<Text> values,
                  Context context
                  ) throws IOException, InterruptedException {
	   /* we should check wether page exists  and whether it has any outlinks in this reducer phase before */
	   
   	boolean page_exists = false;
   	String outlinksdelimeter = "";
   	String[] outlinks;
   	double raw_pagerank =0.0;
   	double finalpagerank =0.0;
   
   	for (Text val : values) {
   		String line = val.toString();
   		//check if the title exists by findingout the delimeter which is sent along with title in the above mapper phase
        if(line.equals("#A#A")){
        	page_exists = true;
        	continue;
        }
        //check whether any outlinks exists (some of the empty outlinks might be written alongside delimeter
        if(line.startsWith("#$#$")){
        	 outlinks =line.split("#\\$#\\$");
        	 String delimeter = "$$$";
        	 //further check whether the delimeter is followed by any outlink
        	 if(outlinks.length>0){
        	 outlinksdelimeter = delimeter+outlinks[1];
        	 continue;
        	 }
        }
        //compute the addition of all pageranks of outlinks only if the value exists
        if(!line.equals(null)){
        raw_pagerank = raw_pagerank+Double.parseDouble(line);
        }
      }
   	//check if page exists
   	if(page_exists){
   		//compute pagerank with damping factor
   		finalpagerank = (new Double(1-0.85))+(0.85)*(raw_pagerank);
   		String result = Double.toString(finalpagerank)+outlinksdelimeter;
  		context.write(key,new Text(result));
   	}
	   
}
}
//In this phase we sort the results returned from above phase
public static class ThirdMapper
extends Mapper<LongWritable, Text,DoubleWritable,Text> {

public void map( LongWritable key, Text value, Context context
             ) throws IOException, InterruptedException {
	//get the title and pagerank value by splitting the input value taken by mapper
       	  String line = value.toString();
       	  String[] line_split = line.split("\t");
	      String[] pr_outlinks_split = line_split[1].split("\\$\\$\\$");
	      Double pageranks = Double.parseDouble(pr_outlinks_split[0].trim());
	      /*parse pagerank as key which helps in sorting phase of reducer and multiply it by (-1)
	      As reducer by default sort ascending order multiplications helps in reverse sorting*/
	      context.write(new DoubleWritable(pageranks*(-1.0)),new Text(line_split[0]));
}
}

public static class ThirdReducer
extends Reducer<DoubleWritable,Text,Text,Text> {
 public void reduce(DoubleWritable key, Iterable<Text> values,
                Context context
                ) throws IOException, InterruptedException {
	 //get the key and values 
 	Text title = null;
 	for(Text val :values){
 		title = val;
 		//multiply again with (-1) to return the original value
 		context.write(title,new Text(String.valueOf(key.get() * (-1.0))));
 	}   
}
}
//main method where we execute all the jobs concerned with the mapreduce tasks stated above
@SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
	int result = 0;
	//jobcount is where we execute mapreduce to get the count of the valid webpages
	 Configuration conf = new Configuration();
	    Job jobcount = Job.getInstance(conf, "GetCount");
	    jobcount.setJarByClass(Test_pr.class);
	    //set class of your map task
	    jobcount.setMapperClass(CountMapper.class);
	    //set class of your reduce task
	    jobcount.setReducerClass(CountReducer.class);
	    //set the output key and values that are returned from the map phase
	    jobcount.setOutputKeyClass(Text.class);
	    jobcount.setOutputValueClass(IntWritable.class);
	    jobcount.setNumReduceTasks(1);
	    FileInputFormat.addInputPath(jobcount, new Path(args[0]));
	    FileOutputFormat.setOutputPath(jobcount, new Path(args[1]));
	    jobcount.waitForCompletion(true);
	    
	//store the value returned from method getcount
    String linecount = getcount(new Path(args[1]+"/part-r-00000"));
    //set the value that is returned ( we use use this value in claculating initial pagerank)
    conf.set("numberoflines",linecount);
    
    //this job handles execution of mapreduce for calculating intial pagerank
    Job job = Job.getInstance(conf, "PageRank");
    job.setJarByClass(Test_pr.class);
    job.setMapperClass(InitialprMapper.class);
    job.setReducerClass(InitialprReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]+"final0"));
    job.waitForCompletion(true);;
    //delete the intermediate files 
    (FileSystem.get(conf)).delete(new Path(args[1]));
    
    //to iterate the pagerank by 10 times we loop the job 10 times.
    for(int i=0;i<10;i++){
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "PageRank");
    job2.setJarByClass(Test_pr.class);
    job2.setMapperClass(SecondMapper.class);
    job2.setReducerClass(SecondReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]+"final"+i));
    // the output of onejob is used as input of another job
    FileOutputFormat.setOutputPath(job2, new Path(args[1]+"final"+(i+1)));
    job2.waitForCompletion(true);
    if(i<10){
    	//delete all the intermediate files except the final one which is carried to next mapreduce
    	(FileSystem.get(conf2)).delete(new Path(args[1]+"final"+i));
    }
        }
    //job execution for sorting phase
    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "PageRank");
    job3.setJarByClass(Test_pr.class);
    job3.setMapperClass(ThirdMapper.class);
    job3.setReducerClass(ThirdReducer.class);
    job3.setOutputKeyClass(DoubleWritable.class);
    job3.setOutputValueClass(Text.class);
    //this setNumreduceTasks helps in merging all the outputs when we execute on cluster
    job3.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job3, new Path(args[1]+"final10"));
    FileOutputFormat.setOutputPath(job3, new Path(args[2]));
    result=(job3.waitForCompletion(true)? 0 : 1);
    (FileSystem.get(conf3)).delete(new Path(args[1]+"final10"));
    //exit the job when all the mapreduces are performed
    System.exit(result);
  }
  //this method helps in reading the output file generated by intial mapreduce task which counts the number of valid webpages
  private static String getcount(Path file) throws IOException{
	 int n = 1;
	 //file system buffer initialization 
 	 FileSystem fs = FileSystem.get(new Configuration());
 	 //read the file using buffer reader
 	 BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)));
 	 String line;
 	 //check whether the line is empty or not and get the count value
 	 while ((line = br.readLine()) != null) {
 		 if (line.trim().length() > 0) {
 			 //split on multiple spaces( we can "\t" instead which is standard output delimeter for key , value pair )
 		     n = Integer.parseInt(line.split("\\s+")[1]);
 		    }
 		   }
      return String.valueOf(n);

  }
}
