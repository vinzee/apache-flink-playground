package com.data_ingestion;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;


public class KafkaDataIngestion {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "127.0.0.1:9092");

        DataStream<String> kafkaData = env.addSource(new FlinkKafkaConsumer011("test", new SimpleStringSchema(), p));

        kafkaData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                String[] words = value.split(" ");
                for (String word : words)
                    out.collect(new Tuple2<String, Integer>(word, 1));
            }
        })

                .keyBy(0)
                .sum(1)
                .writeAsText("/home/jivesh/kafka.txt");

        env.execute("Kafka Example");
    }

}

