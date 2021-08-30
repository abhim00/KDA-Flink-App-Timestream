# Publishing data with Amazon Kinesis to send data to Amazon Timestream

** This repo mainly serves as a POC for a very small part of the work I did at AWS as an intern this summer **

1. Install and configure Boto3 set up following the instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/index.html or executing the following command:
	```
	pip3 install boto3
	```

1. Install numpy 
	```
	pip3 install numpy
	```

1. Run the following command to continuously generate and ingest sample data into Timestream. 
   
    ```    
    python3 timestream_kinesis_data_gen.py --stream <name of the kinesis stream> --region <Specify the region of the Kinesis Stream.> 
    ```
    
    - The ingestion can be stopped with a SIGINT signal (typically, `Ctrl + C` on most systems).
    - Use `--help` argument to see more options available in the Sample Continuous Data Ingestor Python Application.
     E.g. ```python3 timestream_kinesis_data_gen.py --help```
    
### Examples

- Start sending a stream of events to Kinesis stream TimestreamTestStream 

    ```
    python3 timestream_kinesis_data_gen.py --stream TimestreamTestStream --region us-east-1
    ```

- Start sending a stream of events where a percentage of the events will be late arriving by ten minutes

    ```
    python3 timestream_kinesis_data_gen.py --stream TimestreamTestStream --region us-east-1 --late-percent 25 --late-time 600
    ```
