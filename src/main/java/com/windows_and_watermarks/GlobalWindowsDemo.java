package com.windows_and_watermarks;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

public class GlobalWindowsDemo {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        // month, product, category, profit, count
        // tuple  [June,Category5,Bat,12,1]
        // 01-06-2018,June,Category5,Bat,12
//    June    Category5      Bat                      12
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map((MapFunction<String, Tuple5<String, String, String, Integer, Integer>>) value -> {
            // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
            String[] words = value.split(",");
            // ignore timestamp, we don't need it for any calculations
            //Long timestamp = Long.parseLong(words[5]);
            return new Tuple5<>(words[1], words[2], words[3], Integer.parseInt(words[4]), 1);
        }).returns(new TypeHint<Tuple5<String, String, String, Integer, Integer>>() {
        });

        //        [June,Category4,Perfume,10,1]
        // groupBy 'month'
        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped
                .keyBy(0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(5))
                .reduce((ReduceFunction<Tuple5<String, String, String, Integer, Integer>>) (current, pre_result) ->
                        new Tuple5<>(current.f0, current.f1, current.f2, current.f3 + pre_result.f3, current.f4 + pre_result.f4))
                .returns(new TypeHint<Tuple5<String, String, String, Integer, Integer>>() {
                });
        // June { [Category5,Bat,12,1] Category4,Perfume,10,1}	//rolling reduce
        // reduced = { [Category4,Perfume,22,2] ..... }
        reduced.print();

        // execute program
        env.execute("Avg Profit Per Month");
    }
}
