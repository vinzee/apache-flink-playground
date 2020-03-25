package p1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Iterate {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Long, Integer>> data = env.generateSequence(0, 5).map((MapFunction<Long, Tuple2<Long, Integer>>) value -> new Tuple2<>(value, 0));

        // prepare stream for iteration
        IterativeStream<Tuple2<Long, Integer>> iteration = data.iterate(5000);   // ( 0,0   1,0  2,0  3,0   4,0  5,0 )

        // define iteration
        DataStream<Tuple2<Long, Integer>> plusOne = iteration.map((MapFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>>) value -> {
            if (value.f0 == 10)
                return value;
            else
                return new Tuple2<>(value.f0 + 1, value.f1 + 1);
        });   //   plusone    1,1   2,1  3,1   4,1   5,1   6,1

        // part of stream to be used in next iteration (
        DataStream<Tuple2<Long, Integer>> notEqualtoten = plusOne.filter((FilterFunction<Tuple2<Long, Integer>>) value -> value.f0 == 10);
        // feed data back to next iteration
        iteration.closeWith(notEqualtoten);

        // data not feedback to iteration
        DataStream<Tuple2<Long, Integer>> equaltoten = plusOne.filter((FilterFunction<Tuple2<Long, Integer>>) value -> value.f0 == 10);

        equaltoten.writeAsText("/home/jivesh/ten");

        env.execute("Iteration Demo");
    }
}

