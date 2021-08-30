import argparse
import json
import random
import signal
import string
import sys
import time
from collections import namedtuple
import gzip

import boto3
import numpy as np

regions = {
    "us_east_1": dict(cells_per_region=15, silos_per_cell=3),
    "us-east-2": dict(cells_per_region=2, silos_per_cell=2),
    "us-west-1": dict(cells_per_region=6, silos_per_cell=2),
    "us-west-2": dict(cells_per_region=2, silos_per_cell=2),
    "eu-west-1": dict(cells_per_region=10, silos_per_cell=2),
    "ap-northeast-1": dict(cells_per_region=5, silos_per_cell=3),
}


def create_service_log_record(aws_acct_id, timestamp, latency, caller_service, operation):
    sample_log_input = ("------------------------------------------------------------------------\n"\
                "Operation="+operation+"\n"\
                "AwsAccountId="+str(aws_acct_id)+"\n"\
                "HttpStatusCode=200\n"\
                "CallerService="+caller_service+"\n"\
                "Size=2\n"\
                "Time="+latency+"\n"\
                "EndTime="+str(timestamp)+"\n"\
                "StartTime="+str(timestamp)+"\n"\
                "Program=AmazonDataCatalog\n"\
                "EOE")
    return sample_log_input

aws_acct_id_list = ["111111111111", "222222222222", "333333333333", "444444444444", "555555555555", "666666666666"]
timestamp_list = ["Thu, 27 May 2021 20:48:45 UTC", "Fri, 21 May 2021 21:48:45 UTC", "Tue, 12 May 2021 10:48:45 UTC", "Wed, 4 May 2021 23:48:45 UTC"]
caller_service_list = ["GLUE", "S3"]
latency_list = ["178.715432 ms", "123.152632 ms", "562.789562 ms", "125.785214 ms", "252.123568 ms"]
operation_list = ["GetTable", "CreateTable", "CreateNameSpace", "GetDatabase", "CreateDatabase"]


def send_records_to_kinesis(kinesis_client, stream_name, sleep_time, percent_late, late_time):
    count = 0
    late_records_count = 0
    ontime_records_count = 0
    while True:
        if percent_late > 0:
            value = random.random()*100
            if (value >= percent_late):
                print("Generating On-Time Records.")
                ontime_records_count = ontime_records_count + 1
                local_timestamp = int(time.time()) * 1000
            else:
                print("Generating Late Records.")
                late_records_count = late_records_count + 1
                local_timestamp = (int(time.time()) - late_time) * 1000
        else:
            ontime_records_count = count
            local_timestamp = int(time.time()) * 1000
            #local_timestamp = 162214852 + random.randrange(1000, 10000, 4)
        #aws_acct_id, timestamp, latency, caller_service, operation

        records = []
        # chosen_aws_acct_id = random.choice(aws_acct_id_list)
        chosen_aws_acct_id = random.randrange(100000000000, 1000000000000, 12)
        chosen_timestamp = random.choice(timestamp_list)
        chosen_latency = random.choice(latency_list)
        chosen_caller_service = random.choice(caller_service_list)
        chosen_operaton = random.choice(operation_list)

        data = create_service_log_record(chosen_aws_acct_id, local_timestamp, chosen_latency, chosen_caller_service, chosen_operaton)
        gzippedData = gzip.compress(bytes(data, 'utf-8'))
        #print("This is what gzip data looks printed in terminal: {}".format(gzippedData))
        records.append({'Data': gzippedData, 'PartitionKey': str(chosen_aws_acct_id)})
        kinesis_client.put_records(StreamName=stream_name, Records=records)
        count = count + 1
        #print("Wrote {} records to Kinesis Stream '{}'".format(len(records), stream_name))
        print("Wrote {} records so far... to {}; {} are late and {} are on time".format(count, stream_name, late_records_count, ontime_records_count))
        
        if sleep_time > 0:
            time.sleep(float(sleep_time)) 

def main(args):

    print(args)

    def signal_handler(sig, frame):
        print("Exiting Application")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    stream_name = args.stream
    region_name = args.region
    kinesis_client = boto3.client('kinesis', region_name=region_name)

    sleep_time = args.sleep_time
    percent_late = args.percent_late
    late_time = args.late_time

    try:
        kinesis_client.describe_stream(StreamName=stream_name)
    except:
        print("Unable to describe Kinesis Stream '{}' in region {}".format(stream_name, region_name))
        sys.exit(0)

    send_records_to_kinesis(kinesis_client, stream_name, sleep_time, percent_late, late_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='timestream_kinesis_data_gen',
                                     description='Service Log Sample Data Generator for Timestream/KDA Sample Application.')

    parser.add_argument('--stream', action="store", type=str, default="TimestreamTestStream",
                        help="The name of Kinesis Stream.")
    parser.add_argument('--region', '-e', action="store", choices=['us-east-1', 'us-east-2', 'us-west-2', 'eu-west-1'],
                        default="us-east-1", help="Specify the region of the Kinesis Stream.")
    parser.add_argument('--host-scale', dest="hostScale", action="store", type=int, default=1,
                        help="The scale factor determines the number of hosts emitting events and metrics.")
    parser.add_argument('--profile', action="store", type=str, default=None, help="The AWS Config profile to use.")

    # Optional sleep timer to slow down data
    parser.add_argument('--sleep-time', action="store", type=int, default=0,
                        help="The amount of time in seconds to sleep between sending batches.")

    # Optional "Late" arriving data parameters
    parser.add_argument('--percent-late', action="store", type=float, default=0,
                        help="The percentage of data written that is late arriving ")
    parser.add_argument("--late-time", action="store", type=int, default=0,
                        help="The amount of time in seconds late that the data arrives")

    main(parser.parse_args())
