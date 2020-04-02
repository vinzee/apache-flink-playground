package com.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTypesDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> data = env.socketTextStream("localhost", 9090);
        data
            .map((MapFunction<String, Tuple2<Long, String>>) s -> {
                String[] words = s.split(",");
                return new Tuple2<>(Long.parseLong(words[0]), words[1]);
            }).returns(new TypeHint<Tuple2<Long, String>>() {
        })
            .keyBy(0)
            .flatMap(new StatefulMap())
            .print();

        // execute program
        env.execute("State");
    }

    public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Tuple3<String, Long, Integer>> {
        private transient ValueState<Integer> count;
        private transient ReducingState<Long> sum;
        private transient ListState<Long> numbers;

        @Override
        public void flatMap(Tuple2<Long, String> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
            Long currValue = Long.parseLong(input.f1);
            int currCount = count.value();

            currCount += 1;

            count.update(currCount);
            numbers.add(currValue);
            sum.add(currValue);

            if (currCount >= 10) {
                StringBuilder numbersStr = new StringBuilder();
                for (Long number : numbers.get()) {
                    numbersStr.append(number);
                }
                /* emit sum of last 10 elements */
                out.collect(new Tuple3<>(numbersStr.toString(), sum.get(), count.value()));
                /* clear value */
                count.clear();
                sum.clear();
                numbers.clear();
            }
        }

        @Override
        public void open(Configuration conf) {
            numbers = getRuntimeContext().getListState(
                new ListStateDescriptor<>("numbers", Long.class)
            );

            sum = getRuntimeContext().getReducingState(
                new ReducingStateDescriptor<>("reducing sum", (ReduceFunction<Long>) Long::sum, Long.class)
            );

            count = getRuntimeContext().getState(
                new ValueStateDescriptor<>("count", Integer.class)
            );
        }
    }
}


