# Backend Engineering Challenge

This repository contains solution for the software problem posted in [Unbabel](https://github.com/Unbabel/backend-engineering-challenge). 

I have implemented 2 scenarios for the problem statement - a spark streaming application and a python cli implementation.

## SPARK Streaming

The software problem specified is a stateful streaming problem where we need to calculate aggregate data for sliding time windows. The production use case will be a directory with log files or a real time stream of message queue(probably kafka or RabbitMQ). In this application, I have used spark streaming framework to calculate aggregates. The input is a directory with 1 or more json files and output is streamed to json files in an output directory and to console for better understanding. You need scala [sbt](https://www.scala-sbt.org/) and Java. You can run the project by using java or sbt.

To run using java follow these commands from the project root folder. An uber fat jar named unbabel.jar will be generated in folder jar

```
sbt assembly
java -jar ./jar/unbabel.jar --input_directory input_data --window_size 10 --output_directory out
```

To run with sbt use this command from project root folder

```
sbt "run --input_directory input_data --window_size 10 --output_directory out"
```


Spark Streaming has restrictions reading and writing to/from a single file due its distributive nature. Also if you need to run the program again, then you may need to delete the output folder and the checkpoint-dir folder since the application will start from where it left last time. Anyways, for an ideal production scenario, the output will most probably be streamed to a kafka Queue or written to a database table. This will be ideal or those kind of scenarios.


## Python Implementation

However, if specific use case as mentioned in the problem statement is required, please check out the following python implementation 
  
```
python3 aggregator.py --input_file ./input_data/sample_input.json --window_size 10 --output_file out.json
```

In both the cases window_size provided is assumed to be in minutes