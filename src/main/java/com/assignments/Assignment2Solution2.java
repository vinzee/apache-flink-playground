package com.assignments;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;

public class Assignment2Solution2 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("/home/jivesh/ip_data.txt");

        // Join with country codes
        DataStream<String> countryCodes =
            env.readTextFile("/home/jivesh/country_codes.txt");

        MapStateDescriptor<String, String> countryCodesStateDescriptor =
            new MapStateDescriptor<String, String>("country_codes", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        BroadcastStream<String> countryCodesBroadcast = countryCodes.broadcast(countryCodesStateDescriptor);

        DataStream<String> dataWithCountryCodes =
            data
                .connect(countryCodesBroadcast)
                .process(new BroadcastProcessFunction<String, String, String>() {

                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        String inputCountryId = value.split(",")[3];
                        if (ctx.getBroadcastState(countryCodesStateDescriptor).contains(inputCountryId)) {
                            String countryCode = ctx.getBroadcastState(countryCodesStateDescriptor).get(inputCountryId);
                            String valueWithCountryCode = value.replace(inputCountryId, countryCode);
                            out.collect(valueWithCountryCode);
                        }
                    }

                    @Override
                    public void processBroadcastElement(String countryCode, Context ctx, Collector<String> out) throws Exception {
                        String countryId = countryCode.split(",")[0];
                        ctx.getBroadcastState(countryCodesStateDescriptor).put(countryId, countryCode);
                    }
                });

        dataWithCountryCodes.print();

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
                return country.equals("US");
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
            .timeWindow(Time.seconds(10))
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
            .timeWindow(Time.seconds(10))
            .maxBy(1);

        maxClicks.print();

        // website with min clicks
        DataStream<Tuple2<String, Integer>> minClicks =
            clicksPerWebsite
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .minBy(1);

        minClicks.print();

        // distinct users on each website
        DataStream<Tuple2<String, Integer>> usersPerWebsite =
            usStream
                .keyBy(t -> t.f0)
                .timeWindow(Time.seconds(10))
                .process(new ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple2<String, String>> input,
                                        Collector<Tuple2<String, Integer>> out) {
                        ArrayList<String> allUsers = new ArrayList<String>();
                        for (Tuple2<String, String> element : input) {
                            allUsers.add(element.f1);
                        }

                        int distictUsers = (new ArrayList<String>(new HashSet<String>(allUsers))).size();
                        out.collect(new Tuple2<String, Integer>(key, distictUsers));
                    }
                });

        usersPerWebsite.print();

        // execute program
        env.execute("Streaming Click");
    }
}
