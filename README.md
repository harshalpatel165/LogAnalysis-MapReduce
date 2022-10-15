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
  
