/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.operators.LogsToTimestreamPayloadFn;
import com.amazonaws.services.kinesisanalytics.utils.ParameterToolUtils;
import com.amazonaws.services.timestream.TimestreamPoint;
import com.amazonaws.services.timestream.TimestreamSink;

import main.java.com.amazonaws.services.timestream.TimestreamInitializer;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

	/*TODO: update DEFAULT_STREAM_NAME and confirm region*/
	private static final String DEFAULT_STREAM_NAME = "TimestreamTestStream";
	private static final String DEFAULT_REGION_NAME = "us-east-1";

	public static DataStream<String> createKinesisSource(StreamExecutionEnvironment env, ParameterTool parameter) throws Exception {

		//set Kinesis consumer properties
		Properties kinesisConsumerConfig = new Properties();
		//set the region the Kinesis stream is located in
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION,
				parameter.get("Region", DEFAULT_REGION_NAME));
		//obtain credentials through the DefaultCredentialsProviderChain, which includes the instance metadata
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");

		String adaptiveReadSettingStr = parameter.get("SHARD_USE_ADAPTIVE_READS", "false");

		if(adaptiveReadSettingStr.equals("true")) {
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS, "true");
		} else {
			//poll new events from the Kinesis stream once every second
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
					parameter.get("SHARD_GETRECORDS_INTERVAL_MILLIS", "1000"));
			// max records to get in shot
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
					parameter.get("SHARD_GETRECORDS_MAX", "10000"));
		}

		//create Kinesis source
		DataStream<String> kinesisStream = env.addSource(new FlinkKinesisConsumer<>(
				//read events from the Kinesis stream passed in as a parameter
				parameter.get("InputStreamName", DEFAULT_STREAM_NAME),
				//deserialize events with EventSchema
				new SimpleStringSchema(),
				//using the previously defined properties
				kinesisConsumerConfig
		))
				.name("KinesisSource");


		return kinesisStream;
	}

	public static void main(String[] args) throws Exception {
		ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);

		// set up the streaming execution environment, set Time Characteristic to Event Time
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<String> input = createKinesisSource(env, parameter);
		DataStream<TimestreamPoint> mappedInput =
				input
					.map(new LogsToTimestreamPayloadFn())
					.name("ServiceLog-Map-to-Timestream Payload");

		LOG.warn("Finished initial mapping of function");

		//handle late data arrival by setting eventTime, TumblingWindow, SideOutputStream
		final OutputTag<TimestreamPoint> outputLateTag = new OutputTag<TimestreamPoint>("side-output-late") {};
		//final OutputTag<String> outputLateTag = new OutputTag<String>("side-output-late") {};
		//final OutputTag<TimestreamPoint> outputErrTag = new OutputTag<TimestreamPoint>("side-output-err") {};

		WatermarkStrategy<TimestreamPoint> watermarkStrategy = WatermarkStrategy
				.<TimestreamPoint>forMonotonousTimestamps() //builtin watermark generator, simplest special case where timestamps form producer occur in ascending order
				.withTimestampAssigner((timestreamPoint, l) -> timestreamPoint.getTime());

//				.withTimestampAssigner(new SerializableTimestampAssigner<TimestreamPoint>() {
//					@Override
//					public long extractTimestamp(TimestreamPoint timestreamPoint, long l) {
//						l = timestreamPoint.getTime();
//						return l;
//					}
//				});
		DataStream<TimestreamPoint> withTimestampsandWatermarks = mappedInput
				.assignTimestampsAndWatermarks(watermarkStrategy);

		LOG.warn("Right about to execute process function");
		SingleOutputStreamOperator<TimestreamPoint> result = withTimestampsandWatermarks
				.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))) //why 10 seconds? What is the right window size to use?
				.allowedLateness(Time.seconds(5))
				.sideOutputLateData(outputLateTag)
				.process(new ProcessAllWindowFunction<TimestreamPoint, TimestreamPoint, TimeWindow>() {
					//process function erroring is my guyes
					@Override
					public void process(Context context, Iterable<TimestreamPoint> iterable, Collector<TimestreamPoint> collector) throws Exception
					{
						for (TimestreamPoint point: iterable){
							if (point.getTime() < context.window().getStart()) {

								context.output(outputLateTag, point);
							} else {
								collector.collect(point);
							}
						}

					}
				});
		LOG.warn("Finished executing process function");
		result.getSideOutput(outputLateTag).print();


		String region = parameter.get("Region", "us-east-1").toString();
		String databaseName = parameter.get("TimestreamDbName", "kdaflink").toString();
		String tableName = parameter.get("TimestreamTableName", "kinesisdata1").toString();
		int batchSize = Integer.parseInt(parameter.get("TimestreamIngestBatchSize", "50"));

		TimestreamInitializer timestreamInitializer = new TimestreamInitializer(region);
		timestreamInitializer.createDatabase(databaseName);
		timestreamInitializer.createTable(databaseName, tableName);

		SinkFunction<TimestreamPoint> sink = new TimestreamSink(region, databaseName, tableName, batchSize);
		result.addSink(sink).name("Amazon Timestream Sink");
		//mappedInput.addSink(sink);

		// execute program
		env.execute("CloudWatch to KDA to Timestream Flink App");
	}
}
