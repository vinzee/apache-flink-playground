package com.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CreateQueryableState {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
        config.setInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS, 1);
        config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 1);
        config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 1);

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        DataStream<Long> sum = data.map((MapFunction<String, Tuple2<Long, String>>) s -> {
            String[] words = s.split(",");
            return new Tuple2<>(Long.parseLong(words[0]), words[1]);
        })
                .keyBy(0)
                .flatMap(new StatefulMap());
        sum.print();

        // execute program
        env.execute("State");
    }

    public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long> {
        private transient ValueState<Long> sum;            // 2
        private transient ValueState<Long> count;          //  4

        @Override
        public void flatMap(Tuple2<Long, String> input, Collector<Long> out) throws Exception {
            Long currCount = count.value();        //   2
            Long currSum = sum.value();             //  4

            currCount += 1;
            currSum = currSum + Long.parseLong(input.f1);

            count.update(currCount);
            sum.update(currSum);

            if (currCount >= 10) {
                /* emit sum of last 10 elements */
                out.collect(sum.value());
                /* clear value */
                count.clear();
                sum.clear();
            }
        }

        @Override
        public void open(Configuration conf) {
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("sum", Long.class);
            descriptor.setQueryable("sum-query");
            sum = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<>("count", Long.class);
            //descriptor2.setQueryable("count-query");
            count = getRuntimeContext().getState(descriptor2);
        }
    }
}


