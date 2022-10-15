# Logfile Analysis using Hadoop Map Reduce - 
## Introduction
The goal of this application is to use a generated log file with random sequences of characters as log messages, and processes the file and extract different distributions of the messages. These proccess are run in parellel using the hadoop map reduce framework. The log file is initially split into shards, and is run through 4 map reduce tasks which process the data into different distributions. The map reduce tasks are also run through AWS EMR as well.
## Prerequisites and Installation
### Tools and Software 
IntelliJ IDEA 2022.2.1, Scala 3.2.0, Hadoop 3.3.4, Sbt 1.7.1, JDK 11.0.16.1
### Installation
#### Setup and Compile
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

