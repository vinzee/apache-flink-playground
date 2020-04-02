package com.assignments;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * Assignment 1
 * For every 10 second find out for US country
 * a.) total number of clicks on every website
 * b.) the website with maximum number of clicks.
 * c.) the website with minimum number of clicks.
 * d.) Calculate number of distinct users on every website.
 * e.) Calculate the average time spent on website by users.
 * <p>
 * Data Format -
 * user_id,network_name,user_IP,user_country,website, Time spent before next click
 * id_4037,Claro Americas,590673587832453,BR,www.futurebazar.com,49
 */
public class Assignment2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> data = env.readTextFile("/Users/vinzee/code/apache-flink-playground/src/main/java/com/assignments/Assignment2-IP-data.txt");

//         * a.) total number of clicks on every website
        DataStream<Tuple2<String, Integer>> clicksByWebsite = data
            .map(new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String line) {
                    String[] words = line.split(",");
                    return new Tuple2<>(words[4], 1);
                }
            })
            .keyBy(0)
            .window(TumblingProcessingTimeWindows.of(Time.milliseconds(2)))
            .sum(1);
        clicksByWebsite.print("Clicks by Website");

//         * b.) the website with maximum number of clicks.
        clicksByWebsite
            .keyBy(0)
            .window(TumblingProcessingTimeWindows.of(Time.milliseconds(2)))
            .maxBy(1)
            .print("Maximum number of clicks");

//         * c.) the website with minimum number of clicks.
        clicksByWebsite
            .keyBy(0)
            .window(TumblingProcessingTimeWindows.of(Time.milliseconds(2)))
            .minBy(1)
            .print("Minimum number of clicks");

//         * d.) Calculate number of distinct users on every website.
        data
            .map(new MapFunction<String, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> map(String line) {
                    String[] words = line.split(",");
                    return new Tuple2<>(words[4], words[0]);
                }
            })
            .keyBy(0)
            .flatMap(new DistinctUsers())
            .print("distinct users per website");

//         * e.) Calculate the average time spent on website by users.
        data
            .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                @Override
                public Tuple3<String, Integer, Integer> map(String line) {
                    String[] words = line.split(",");
                    return new Tuple3<>(words[4], Integer.parseInt(words[5]), 1);
                }
            })
            .keyBy(0)
            .window(TumblingProcessingTimeWindows.of(Time.milliseconds(2)))
            .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                @Override
                public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> t1, Tuple3<String, Integer, Integer> t2) throws Exception {
                    return new Tuple3<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2);
                }
            })
            .map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> t) throws Exception {
                    return new Tuple2<>(t.f0, t.f1 * 1.0 / t.f2);
                }
            })
            .print("Average Time by users on websites");


        env.execute("IPStreamProcessor");
    }

    private static class DistinctUsers extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>> {
        private transient MapState<String, Integer> usersState;

        @Override
        public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            usersState.put(input.f1, 1);

            int sum = 0;
            for (String _key : usersState.keys()) {
                ++sum;
            }
            out.collect(new Tuple2<>(input.f0, sum));
        }

        @Override
        public void open(Configuration conf) {
            MapStateDescriptor<String, Integer> desc = new MapStateDescriptor<>("users_state", String.class, Integer.class);
            usersState = getRuntimeContext().getMapState(desc);
        }
    }

}