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

package mypackage;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;



public class DataStreamJob {




	public static void main(String[] args) throws Exception {


		ParameterTool parameters = ParameterTool.fromArgs(args);
		int dimensions = parameters.getInt("d");
		int parallelism = parameters.getInt("w");
		int maxValue = parameters.getInt("v");
		int numOfPartitions = parameters.getInt("p"); //per dimension in case of grid
		String algorithm = parameters.get("a"); //grid, angle, dim
		String inputTopic = parameters.get("it");
		String outputTopic = parameters.get("ot");
		String triggerTopic = parameters.get("tt");


//		int dimensions = 4;
//		int parallelism = 32;
//		int maxValue = 1000000000;
//		int numOfPartitions = 2;
//		String algorithm = "angle";


		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics(inputTopic)
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		KafkaSource<String> triggerSource = KafkaSource.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics(triggerTopic)
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(outputTopic)
						.setValueSerializationSchema(new SimpleStringSchema())
						.build()
				)
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();



		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		DataStream<String> inputData = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
		DataStream<String> inputTrigger = env.fromSource(triggerSource, WatermarkStrategy.noWatermarks(), "Kafka Trigger Source");
//		DataStream<String> inputData = env.fromElements("9 1", "3 7", "2 6", "5 5", "4 4", "7 7", "6 2", "1 9");
//		DataStream<String> inputData = env.fromElements("9 1 1", "3 7 3", "2 2 6", "5 5 5", "4 4 7", "7 7 3");
//		DataStream<String> inputData = env.fromElements("3 3");
//		DataStream<String> inputData = env.fromElements("3 3", "6 6", "2 6", "6 3");


		DataStream<List<Integer>> triggers = inputTrigger
				.flatMap(new TriggerFormatter(numOfPartitions, algorithm, maxValue, dimensions))
				.keyBy(new DimKeySelector(algorithm, numOfPartitions, maxValue, dimensions));

		DataStream<List<Integer>> services = inputData
				.map(new StringToListFunction())
				.map(new HashFunction(dimensions, numOfPartitions, maxValue, algorithm))
				.keyBy(new DimKeySelector(algorithm, numOfPartitions, maxValue, dimensions));


		DataStream<List<Integer>> localSkylines = services
				.connect(triggers)
				.process(new LocalSkylineFunction(dimensions, algorithm));


		DataStream<String> outputStream = localSkylines
				.keyBy(x->1)
				.process(new GlobalSkylineCalculator(dimensions, numOfPartitions, algorithm))
				.map(new PruneKeyFunction());



//		outputStream.writeAsText(algorithm.concat(Integer.toString(dimensions).concat(".txt")), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		outputStream.sinkTo(kafkaSink);
//		outputStream.print();


		env.execute("Flink Java API Skeleton");

	}

}
