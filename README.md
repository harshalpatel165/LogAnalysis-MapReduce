# Logfile Analysis using Hadoop Map Reduce - 
## Introduction
The goal of this application is to use a generated log file with random sequences of characters as log messages, and processes the file and extract different distributions of the messages. These proccess are run in parellel using the hadoop map reduce framework. The log file is initially split into shards, and is run through 4 map reduce tasks which process the data into different distributions. The map reduce tasks are also run through AWS EMR as well.
##
