<h3>Instruction to execute DocWordCount.java, TermFrequency.java, TFIDF.java & Search.java for the cantebury corpus and save results</h3>

<h4>Some common hadoop commands which would be useful :</h4>  
 Create a new directory :
hadoop fs -mkdir /user/cloudera/directoryname<br>     
 Read the output : 
>hadoop fs -cat /user/cloudera/directoryname/filename<br>     
Remove files : 
>hadoop fs -rm /user/cloudera/filename<br>     
Remove Directory : hadoop fs -rm -r /user/cloudera/directoryname<br>     
Push the file into cloud : hadoop fs -put /home/cloudera/file   /user/cloudera/file<br>     
Here "/home/cloudera/file " is local file path and "/user/cloudera/file" is path in the cloudstorage <br>    
Get the file from cloud : hadoop fs -get  /user/cloudera/file /home/cloudera/file <br>     
Here "/home/cloudera/file " is local file path and "/user/cloudera/file" is path in the cloudstorage <br>    


<h5>Preferred steps : </h5>   
Host the Cantebury Corpus in the cloudera by using the command :hadoop fs -put /home/cloudera/CanteburyCorpus /user/cloudera/ <br>    
The argument /home/cloudera/CanteburyCorpus  refers to the path where you store the Cantebury Corpus folder.<br>     

<h4>DocWordCount.java : This program helps to get all the words and their respective counts in a given set of files.</h4>    
<h4>Instructions to execute DocWordCount.java:</h4>
1. Create a JAR file and save it in local folder<br>    
2. Make sure you have the input folder in the cloud<br>     
3. Execute the following command :<br> 
>hadoop jar /home/cloudera/DocWordCount.jar WordCount /user/cloudera/CanteburyCorpus /user/cloudera/DocWordCount<br>    
4. Argument "/home/cloudera/DocWordCount.jar" refers to path of the jar file which is saved locally<br>     
5. "WordCount" is the class name of the DocWordCount.java file<br>     
6. "/user/cloudera/CanteburyCorpus" is the path of input folder which is stored in cloudera<br>     
7. "/user/cloudera/DocWordCount" is path where you store the output<br>     
8.Output folder in general have two files with the names _SUCCESS and part-r-00000. The file with name part-r-00000 has the words and their counts<br>     
9.Get the output file to localdisk using : hadoop fs -get /user/cloudera/DocWordCount /home/cloudera/DocWordCount.<br>     

<h4>TermFrequency.java : This program helps to get all the words and their respective term frequencies in the files they appeared. </h4> 

<h4>Instructions to execute TermFrequency.java:</h4>
1. Create a JAR file and save it in local folder
2.Make sure you have the input folder in the cloud.
3. Execute the following command : hadoop jar /home/cloudera/Termfrequency.jar TermFrequency /user/cloudera/CanteburyCorpus /user/cloudera/TermFrequency
4. Argument "/home/cloudera/TermFrequency.jar" refers to path of the jar file which is saved locally.
5. "Termfrequency" is the class name of the TermFrequency.java file.
6. "/user/cloudera/CanteburyCorpus" is the path of input folder which is stored in cloudera.
7. "/user/cloudera/TermFrequency" is path where you store the output.
8.Output folder in general have two files with the names _SUCCESS and part-r-00000. The file with name part-r-00000 has the words and their termfrequencies.
9.Get the output file to localdisk using : hadoop fs -get /user/cloudera/TermFrequency /home/cloudera/TermFrequency.
10. All the arguments the execution command are case sensitive. Spell check is suggestable before execution.



<h5>TFIDF.java : This program helps to get all the words and their respective TFIDF values in the files they appeared. This file has two mapreduce jobs where output from first mapreduce job is used as input for second mapreduce job.</h5>

<h4>Instructions to execute TFIDF.java:</h4>
1. Create a JAR file and save it in local folder
2.Make sure you have the input folder in the cloud.
3. Execute the following command : hadoop jar /home/cloudera/TFIDF.jar TfIdf /user/cloudera/CanteburyCorpus /user/cloudera/Tfidf_temp /user/cloudera/Tfidf 8
4. Argument "/home/cloudera/TermFrequency.jar" refers to path of the jar file which is saved locally.
5. "Termfrequency" is the class name of the TermFrequency.java file.
6. "/user/cloudera/CanteburyCorpus" is the path of input folder which is stored in cloudera.
7. "/user/cloudera/Tfidf_temp" is path where you store the termfrequency output which used as input for second mapreduce job.
8. "/user/cloudera/Tfidf" is path which stores the final output of the TFIDF.
9. The numeric value which is at the end of the execution code is userinput which can be optimized with the number of files in Corpus. User should enter the number of inputfiles in the inputfolder. Numerics are accpeted where as strings throws a error.
10.Output folder in general have two files with the names _SUCCESS and part-r-00000. The file with name part-r-00000 has the words and their TFIDF values.
11.Get the output file to localdisk using : hadoop fs -get /user/cloudera/Tfidf /home/cloudera/Tfidf.
12. All the arguments the execution command are case sensitive. Spell check is suggestable before execution.



<h5>Search.java : This program helps to get all the files whith their corresponding TFIDF values for a given  search query </h5>

<h4>Instructions to execute Search.java:</h4>
<h4>Part-1</h4>
1. Create a JAR file and save it in local folder
2.Make sure you have the input folder in the cloud.
3. Execute the following command : hadoop jar /home/cloudera/Search.jar Search /user/cloudera/TFIDF/part-r-00000 /user/cloudera/Search "computer science"
4. Argument "/home/cloudera/Search.jar" refers to path of the jar file which is saved locally.
5. "Search" is the class name of the Search.java file.
6. "/user/cloudera/TFIDF/part-r-00000" is the path of input folder which is stored in cloudera. This is input file is the output file of TFIDF.jar execution.
7. "/user/cloudera/Search" is path which stores the final output of the Search.
8. The numeric value which is at the end of the execution code is userinput which can be optimized with the number of files in Corpus. User should enter the number of inputfiles in the inputfolder. Numerics are accpeted where as strings throws a error.
9. Output folder in general have two files with the names _SUCCESS and part-r-00000. The file with name part-r-00000 has the files which have the search query and their resepective TFIDF socres.
10.Get the output file to localdisk using : hadoop fs -get /user/cloudera/Search /home/cloudera/Search.
11. All the arguments the execution command are case sensitive. Spell check is suggestable before execution.

<h4>Part-2 :You use a different search query compared to part 1</h4>
1. Create a JAR file and save it in local folder
2.Make sure you have the input folder in the cloud.
3. Execute the following command : hadoop jar /home/cloudera/Search.jar Search /user/cloudera/TFIDF/part-r-00000 /user/cloudera/Search2 "data analysis"
4. Argument "/home/cloudera/Search.jar" refers to path of the jar file which is saved locally.
5. "Search" is the class name of the Search.java file.
6. "/user/cloudera/TFIDF/part-r-00000" is the path of input folder which is stored in cloudera. This is input file is the output file of TFIDF.jar execution.
7. "/user/cloudera/Search2" is path which stores the final output of the Search.
8. The numeric value which is at the end of the execution code is userinput which can be optimized with the number of files in Corpus. User should enter the number of inputfiles in the inputfolder. Numerics are accpeted where as strings throws a error.
9. Output folder in general have two files with the names _SUCCESS and part-r-00000. The file with name part-r-00000 has the files which have the search query and their resepective TFIDF socres.
10.Get the output file to localdisk using : hadoop fs -get /user/cloudera/Search2 /home/cloudera/Search2.
11. All the arguments the execution command are case sensitive. Spell check is suggestable before execution.
