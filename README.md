# Logfile Analysis using Hadoop Map Reduce - 
## Introduction
The goal of this application is to use a generated log file with random sequences of characters as log messages, and processes the file and extract different distributions of the messages. These proccess are run in parellel using the hadoop map reduce framework. The log file is initially split into shards, and is run through 4 map reduce tasks which process the data into different distributions. The map reduce tasks are also run through AWS EMR as well.
## Prerequisites and Installation
### Tools and Software 
IntelliJ IDEA 2022.2.1, Scala 3.2.0, Hadoop 3.3.4, Sbt 1.7.1, JDK 11.0.16.1
### Installation
#### Setup and Compile
- Note: Make sure all tools are running on administrator mode including command line andd Intellij
- Clone project using Intellij IDEA -> Get from VCS -> URL: ```git@github.com:harshalpatel165/LogAnalysis-MapReduce.git``` -> Directory: directory of your choosing -> Clone
- Using built-in terminal use:

  To compile:
  ```sbt clean compile```
  
  to test:
  ```sbt clean test```
  
  to build Jar:
  ``` sbt clean compile assembly```
 
#### Running locally
- In terminal:
  
  Running task 1:
  
  ```sbt "run 1 [absolute_path_to_input_folder] [absolute_path_to_output_folder1]"```
  
  Running task 2:
  
  ```sbt "run 2 [absolute_path_to_input_folder] [absolute_path_to_temporary_folder] [absolute_path_to_output_folder2]"```
  
  Running task 3:
  
  ```sbt "run 3 [absolute_path_to_input_folder] [absolute_path_to_output_folder3]"```
  
  Running task 4:
  
  ```sbt "run 4 [absolute_path_to_input_folder] [absolute_path_to_output_folder4]"```
  
 Note: The input folder has already been given with a log file that has been generated and split into shards using ```split data.log -l 1000```. It is located in ```src/main/resources/input```
 
Each time these tasks are run there should not be a output folder or temporary folder created, the framework will create it for you. Make sure to delete any output folders already existing or changing the output path folder to a different name that does not exist. It is recommeneded to put the absolute_path_to_output_folder in the resources folder where the input folder is.

To get the absolute paths of the input folder and output folder, on Intellij on the left hand side, expand ```src/main/resources/``` and right click the input folder -> Copy Path/Reference -> Absolute Path.

The output is given in the output folder in a file named part-r-00000. To convert it to csv run 

```cat [absolute_path_to_file] > [absolute_path_to_file.csv]```


#### Running on hadoop
- On command line go to where hadoop home directory is. Start hadoop using ```sbin/start-all.cmd```. 
- Then make sure you have built the project using the assembly command mentioned above on intellij. 
- In the local project directory under ```target/scala-3.2.0``` There should be a jar file there. Copy and note down the absolute path of that jar file.
- then while hadoop is running on the terminal copy the input folder in the local filesystem to the hadoop file system using 

  ```hdfs dfs -copyFromLocal [absolute_path_to_input_folder] /input```
- Running ```hdfs dfs -ls /``` should look something like this: 

  ![image](https://user-images.githubusercontent.com/55267253/196010363-61f7a9c2-bf9c-45b7-9899-e5647849ed25.png)
- Now to run each task use the following commands where the ```[absolute_path_to_jar_file]``` is the path noted in step 3:

  Running task 1:
  
  ```hadoop jar [absolute_path_to_jar_file] 1 /input /output/output1"```
  
  Running task 2:
  
  ```hadoop jar [absolute_path_to_jar_file] 2 /input /output/tmpoutput /output/output2"```
  
  Running task 3:
  
  ```hadoop jar [absolute_path_to_jar_file] 3 /input /output/output3"```
  
  Running task 4:
  
  ```hadoop jar [absolute_path_to_jar_file] 4 /input /output/output4"```
- After a task has fully finished running, doing ```hdfs dfs -ls /output/output[task_number]``` should look like this:

  ![image](https://user-images.githubusercontent.com/55267253/196010608-870f04a8-27f3-4f2c-ab12-9dd2b22d3fb5.png)


