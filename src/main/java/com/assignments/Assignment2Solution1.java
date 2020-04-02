package com.assignments;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Assignment2Solution1 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("/Users/vinzee/code/apache-flink-playground/src/main/java/com/assignments/Assignment2-IP-data.txt");


        // click data keyed by website
        DataStream<Tuple2<String, String>> keyedData = data
            .map(new MapFunction<String, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> map(String value) {
                    String[] words = value.split(",");
                    // <website, all_data>
                    return new Tuple2<String, String>(words[4], value);
                }
            });

        // US click stream only
        DataStream<Tuple2<String, String>> usStream = keyedData.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) {
                String country = value.f1.split(",")[3];
                return !country.equals("US");
            }
        });

        // total number of clicks on every website in US
        DataStream<Tuple2<String, Integer>> clicksPerWebsite = usStream.map(new MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(Tuple2<String, String> value) {
                return new Tuple3<String, String, Integer>(value.f0, value.f1, 1);
            }
        })
            .keyBy(0)
            .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
            .sum(2)
            .map(new MapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(Tuple3<String, String, Integer> value) {
                    return new Tuple2<String, Integer>(value.f0, value.f2);
                }
            });
        clicksPerWebsite.print();

        // website with max clicks
        DataStream<Tuple2<String, Integer>> maxClicks = clicksPerWebsite
            .keyBy(0)
            .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
            .maxBy(1);

        maxClicks.print();

        // website with min clicks
        DataStream<Tuple2<String, Integer>> minClicks =
            clicksPerWebsite
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
                .minBy(1);

        minClicks.print();
        DataStream<Tuple2<String, Integer>> avgTimeWebsite =
            usStream
                .map(new MapFunction<Tuple2<String, String>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(Tuple2<String, String> value) {
                        int timeSpent = Integer.parseInt(value.f1.split(",")[5]);
                        return new Tuple3<String, Integer, Integer>(value.f0, 1, timeSpent);
                    }
                })
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> v1,
                                                                   Tuple3<String, Integer, Integer> v2) {
                        return new Tuple3<String, Integer, Integer>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2);
                    }
                })
                .map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple3<String, Integer, Integer> value) {
                        return new Tuple2<String, Integer>(value.f0, (value.f2 / value.f1));
                    }
                });
        avgTimeWebsite.print();

        // distinct users on each website
        DataStream<Tuple2<String, Integer>> usersPerWebsite = usStream
            .keyBy(0)
            .flatMap(new DistinctUsers());

        usersPerWebsite.print();

        // execute program
        env.execute("Streaming Click");
    }

    public static class DistinctUsers extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>> {
        private transient ListState<String> usersState;

        @Override
        public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            usersState.add(input.f1);

            HashSet<String> distinctUsers = new HashSet<String>();
            for (String user : usersState.get()) {
                distinctUsers.add(user);
            }
            out.collect(new Tuple2<String, Integer>(input.f0, distinctUsers.size()));
        }

        @Override
        public void open(Configuration conf) {
            ListStateDescriptor<String> desc = new ListStateDescriptor<String>("users_state", BasicTypeInfo.STRING_TYPE_INFO);
            usersState = getRuntimeContext().getListState(desc);
        }
    }
}
