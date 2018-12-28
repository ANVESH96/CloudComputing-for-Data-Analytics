README file to compute the pagerank on single node cluster and  dsba-hadoop cluster

Running on single node cluster: ( this method can be used to test the algorithm on graph1.txt , graph2.txt and wiki-micro.txt)
------------------------------------------------------------------------------------------------------------------------------

1. Compute the jar for the file pagerank.java which has pagerank algorithm  
2. Create a directory in cluster using command:  
 >hadoop fs -mkdir /user/cloudera/{directoryname}  
3. Insert the input file into the cluster using command:  
 >hadoop fs -put {file path in local directory} /user/cloudera/  
4. Run the jar file using command :  
 >hadoop jar {filepath of jar in local machine} JAVA_CLASSNAME /user/cloudera/{inputfilename} /user/cloudera/{intermediate file name} /user/cloudera/output   
5. After the output is generated you can get to the desried category using the command below:  
 >hadoop jar -get /user/cloudera/output {desired path in edge node}  
6. The intermediate files are deleted to avoid space wastage.  


Running on Multi-node cluster:
-------------------------------

1.Compute the jar for the file pagerank.java which has pagerank algorithm   
2. Make sure you get the jar file  and input to the edge node of the Cluster using WinScp  
3. Get the input and place it in name node of the cluster using command below:  
>hadoop fs -put {Filepath in local directory/edge node} {file path in hadoop}  
4. Run the jar file using command :  
>hadoop jar {jar path in edge node} JAVA_CLASSNAME {input file path in node} {intermediate file path}{output directory path}  
5. After the output is generated you can get to the desried category using the command below:  
>hadoop jar -get {output file path} {desired path in edge node}  
6. Intermediate files are deleted to avoid space wastage.  
7. To get the first 100 lines of the output use the command below:  
>hadoop fs -cat {outputfilepath in cluster} | head -n 100  

Note: The function Filesystem.delete() is used to remove the intermediate directories created which is deprecated method   
and might not work in some updated hadoop.  
