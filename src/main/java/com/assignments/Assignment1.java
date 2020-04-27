package com.assignments;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Assignment 1
 * Using Datastream/Dataset transformations find the following for each ongoing trip.
 * 1.) Most Popular destination.  | Where more number of people reach.
 * 2.) Average number of passengers from each pickup location.  | average =  total no. of passengers from a location / no. of trips from that location.
 * 3.) Average number of trips for each driver.  | average =  total no. of passengers drivers has picked / total no. of trips he made
 *
 * Data Schema -
 * cab id, cab number plate, cab type, cab driver name, ongoing trip/not, pickup location, destination,passenger count
 */
public class Assignment1 {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> data = env.readTextFile("/Users/vinzee/code/apache-flink-playground/src/main/java/com/assignments/Assignment1-cab-data.txt");

//      Most Popular destination
        data
            .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>> (){
                @Override
                public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    String[] words = input.split(",");
                    if(!"'null'".equals(words[6]) && !"'null'".equals(words[7])){
                        collector.collect(new Tuple2<>(words[6], Integer.parseInt(words[7])));
                    }
                }

            })
            .groupBy(0)
            .sum(1)
            .maxBy(1)
            .print();

        env.execute("Assignment1");
    }

}